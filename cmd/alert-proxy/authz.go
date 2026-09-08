package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"sync"
	"time"

	userv1 "github.com/openshift/api/user/v1"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/types"
	ctrlruntimeclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const authzRefreshInterval = 10 * time.Minute
const authzHardCeiling = 20 * time.Minute

type GroupReader interface {
	Users(context.Context, string) ([]string, error)
}
type KubeGroupReader struct{ Client ctrlruntimeclient.Client }

func (r *KubeGroupReader) Users(ctx context.Context, name string) ([]string, error) {
	group := &userv1.Group{}
	if err := r.Client.Get(ctx, types.NamespacedName{Name: name}, group); err != nil {
		return nil, err
	}
	return append([]string(nil), group.Users...), nil
}

type SlackIdentityResolver interface {
	LookupUserByEmail(context.Context, string) (string, error)
}
type identityMap struct {
	KerberosSlackIDOverrides map[string]string `json:"kerberosSlackIDOverrides"`
}

type Authorizer struct {
	group                   GroupReader
	resolver                SlackIdentityResolver
	groupName, identityPath string
	metrics                 *Metrics
	logger                  *logrus.Entry
	now                     func() time.Time
	mu                      sync.RWMutex
	users                   map[string]bool
	refreshedAt             time.Time
	lastErr                 error
}

func NewAuthorizer(group GroupReader, resolver SlackIdentityResolver, groupName, identityPath string, metrics *Metrics, logger *logrus.Entry) *Authorizer {
	return &Authorizer{group: group, resolver: resolver, groupName: groupName, identityPath: identityPath, metrics: metrics, logger: logger, now: time.Now, users: map[string]bool{}}
}

func (a *Authorizer) Refresh(ctx context.Context) error {
	members, err := a.group.Users(ctx, a.groupName)
	if err != nil {
		a.mu.Lock()
		a.lastErr = err
		a.mu.Unlock()
		return fmt.Errorf("read OpenShift Group %s: %w", a.groupName, err)
	}
	overrides, ignored, err := loadIdentityMapDetailed(a.identityPath, members)
	if err != nil {
		a.mu.Lock()
		a.lastErr = err
		a.mu.Unlock()
		return err
	}
	if a.logger != nil {
		for _, kerberos := range ignored {
			a.logger.WithField("kerberos", kerberos).Warn("Slack identity override does not name a current privileged-group member and was ignored")
		}
	}
	resolved := map[string]bool{}
	unresolved := 0
	for _, kerberos := range members {
		id := overrides[kerberos]
		if id == "" {
			id, err = a.resolver.LookupUserByEmail(ctx, kerberos+"@redhat.com")
			if err != nil {
				unresolved++
				if a.logger != nil {
					a.logger.WithError(err).WithField("kerberos", kerberos).Warn("failed to resolve Slack identity")
				}
				continue
			}
		}
		if resolved[id] {
			err = fmt.Errorf("slack ID %s resolves more than one group member", id)
			a.mu.Lock()
			a.lastErr = err
			a.mu.Unlock()
			return err
		}
		resolved[id] = true
	}
	now := a.now()
	a.mu.Lock()
	a.users = resolved
	a.refreshedAt = now
	a.lastErr = nil
	a.mu.Unlock()
	if a.metrics != nil {
		a.metrics.groupMembersUnresolved.Set(float64(unresolved))
		a.metrics.authzCacheAge.Set(0)
	}
	if a.logger != nil {
		a.logger.WithFields(logrus.Fields{"group": a.groupName, "resolved": len(resolved), "unresolved": unresolved}).Info("refreshed privileged Slack users")
	}
	return nil
}

func loadIdentityMap(path string, current []string) (map[string]string, error) {
	overrides, _, err := loadIdentityMapDetailed(path, current)
	return overrides, err
}

func loadIdentityMapDetailed(path string, current []string) (map[string]string, []string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read Slack identity map: %w", err)
	}
	var cfg identityMap
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, nil, fmt.Errorf("decode Slack identity map: %w", err)
	}
	currentSet := map[string]bool{}
	for _, x := range current {
		currentSet[x] = true
	}
	out := map[string]string{}
	ignored := []string{}
	ids := map[string]string{}
	valid := regexp.MustCompile(`^[UW][A-Z0-9]+$`)
	keys := make([]string, 0, len(cfg.KerberosSlackIDOverrides))
	for k := range cfg.KerberosSlackIDOverrides {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		id := cfg.KerberosSlackIDOverrides[k]
		if !valid.MatchString(id) {
			return nil, nil, fmt.Errorf("identity override for %s has malformed Slack ID", k)
		}
		if other := ids[id]; other != "" {
			return nil, nil, fmt.Errorf("identity overrides for %s and %s duplicate Slack ID %s", other, k, id)
		}
		ids[id] = k
		if !currentSet[k] {
			ignored = append(ignored, k)
			continue
		}
		out[k] = id
	}
	return out, ignored, nil
}

func (a *Authorizer) Authorize(ctx context.Context, actor, action string) error {
	now := a.now()
	a.mu.RLock()
	present := a.users[actor]
	age := now.Sub(a.refreshedAt)
	never := a.refreshedAt.IsZero()
	lastErr := a.lastErr
	a.mu.RUnlock()
	if never || age >= authzRefreshInterval || !present {
		if err := a.Refresh(ctx); err != nil {
			a.mu.RLock()
			present = a.users[actor]
			age = now.Sub(a.refreshedAt)
			a.mu.RUnlock()
			if !present || age > authzHardCeiling {
				return a.decision(actor, action, "group-unavailable", fmt.Errorf("privileged group is unavailable: %w", err))
			}
		}
		a.mu.RLock()
		present = a.users[actor]
		age = now.Sub(a.refreshedAt)
		lastErr = a.lastErr
		a.mu.RUnlock()
	}
	if a.metrics != nil && !never {
		a.metrics.authzCacheAge.Set(age.Seconds())
	}
	if age > authzHardCeiling || lastErr != nil && age > authzHardCeiling {
		return a.decision(actor, action, "group-unavailable", errors.New("privileged group cache is stale"))
	}
	if !present {
		return a.decision(actor, action, "denied", fmt.Errorf("slack user is not a member of %s", a.groupName))
	}
	return a.decision(actor, action, "allowed", nil)
}
func (a *Authorizer) decision(actor, action, result string, err error) error {
	if a.metrics != nil {
		a.metrics.authzDecisions.WithLabelValues(action, result).Inc()
	}
	if a.logger != nil {
		fields := logrus.Fields{"actor": actor, "action": action, "result": result, "group": a.groupName}
		entry := a.logger.WithFields(fields)
		if err != nil {
			entry.WithError(err).Warn("Slack authorization decision")
		} else {
			entry.Info("Slack authorization decision")
		}
	}
	return err
}
func (a *Authorizer) Run(ctx context.Context) {
	ticker := time.NewTicker(authzRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = a.Refresh(ctx)
		}
	}
}
