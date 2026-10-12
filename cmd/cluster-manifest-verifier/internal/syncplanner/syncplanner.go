package syncplanner

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	argov1alpha1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/util/sets"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"

	"github.com/openshift/ci-tools-standalone/cmd/cluster-manifest-verifier/internal/gitchanges"
)

type Plan struct {
	Name      string
	FullSync  bool
	Resources []argov1alpha1.SyncOperationResource
}

type Planner struct {
	repoDir string
}

func New(repoDir string) *Planner {
	return &Planner{repoDir: repoDir}
}

func (p *Planner) Build(apps map[string]argov1alpha1.Application, changes []gitchanges.FileChange) (map[string]*Plan, error) {
	plans := make(map[string]*Plan, len(apps))
	for name := range apps {
		plans[name] = &Plan{Name: name}
	}

	var planErr error
	for _, change := range changes {
		if strings.HasPrefix(change.Path, "clusters/gitops/apps/") {
			continue
		}
		matched := false
		for name, app := range apps {
			if !p.coversPath(app, change.Path) {
				continue
			}
			logrus.Infof("change %q -> Application %q", change.Path, name)
			matched = true
			if err := p.applyChange(plans[name], change); err != nil {
				planErr = errors.Join(planErr, fmt.Errorf("%s (Application %q): %w", change.Path, name, err))
			}
		}
		if !matched {
			// Deleted app dirs are gone from the PR revision, so ApplicationSet
			// generation yields no Application to match against.
			if change.Status == 'D' {
				continue
			}
			planErr = errors.Join(planErr, fmt.Errorf("file %q is not covered by any Application source path", change.Path))
		}
	}

	for name, plan := range plans {
		if plan.FullSync || len(plan.Resources) > 0 {
			continue
		}
		delete(plans, name)
	}
	return plans, planErr
}

func (p *Planner) coversPath(app argov1alpha1.Application, changedPath string) bool {
	if app.Spec.Source != nil && app.Spec.Source.Path != "" {
		if strings.HasPrefix(changedPath, strings.TrimSuffix(app.Spec.Source.Path, "/")+"/") {
			return true
		}
	}
	for _, source := range app.Spec.Sources {
		if source.Path != "" && strings.HasPrefix(changedPath, strings.TrimSuffix(source.Path, "/")+"/") {
			return true
		}
	}
	return false
}

func (p *Planner) applyChange(plan *Plan, change gitchanges.FileChange) error {
	switch change.Status {
	case 'D':
		plan.FullSync = true
		return nil
	case 'R', 'C':
		plan.FullSync = true
		return nil
	}

	lower := strings.ToLower(change.Path)
	if !strings.HasSuffix(lower, ".yaml") && !strings.HasSuffix(lower, ".yml") {
		plan.FullSync = true
		return nil
	}

	resources, err := p.parseManifestResources(filepath.Join(p.repoDir, change.Path))
	if err != nil {
		return err
	}
	if len(resources) == 0 {
		plan.FullSync = true
		return nil
	}
	plan.addResources(resources)
	return nil
}

func (p *Planner) parseManifestResources(path string) ([]argov1alpha1.SyncOperationResource, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	decoder := k8syaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var resources []argov1alpha1.SyncOperationResource
	for {
		var doc struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name      string `json:"name"`
				Namespace string `json:"namespace"`
			} `json:"metadata"`
		}
		if err := decoder.Decode(&doc); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("decode manifest: %w", err)
		}
		if doc.APIVersion == "" || doc.Kind == "" || doc.Metadata.Name == "" {
			continue
		}
		if doc.Kind == "List" {
			return nil, fmt.Errorf("resource List in %s requires full application sync", path)
		}
		group := ""
		if parts := strings.SplitN(doc.APIVersion, "/", 2); len(parts) == 2 {
			group = parts[0]
		}
		resources = append(resources, argov1alpha1.SyncOperationResource{
			Group:     group,
			Kind:      doc.Kind,
			Name:      doc.Metadata.Name,
			Namespace: doc.Metadata.Namespace,
		})
	}
	return resources, nil
}

func (p *Plan) addResources(resources []argov1alpha1.SyncOperationResource) {
	seen := sets.New[string]()
	for _, existing := range p.Resources {
		seen.Insert(resourceKey(existing))
	}
	for _, resource := range resources {
		key := resourceKey(resource)
		if seen.Has(key) {
			continue
		}
		seen.Insert(key)
		p.Resources = append(p.Resources, resource)
	}
}

func resourceKey(resource argov1alpha1.SyncOperationResource) string {
	return resource.Group + "|" + resource.Kind + "|" + resource.Namespace + "|" + resource.Name
}
