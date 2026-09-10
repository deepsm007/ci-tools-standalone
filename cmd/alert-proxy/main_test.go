package main

import (
	"strings"
	"testing"
)

func TestAlertmanagerURLValidation(t *testing.T) {
	base, err := gatherOptions([]string{"--channel-id=C1"})
	if err != nil {
		t.Fatal(err)
	}
	const marker = "do-not-log"
	testCases := []struct {
		name    string
		value   string
		wantErr bool
	}{
		{name: "service", value: "https://alertmanager-user-workload.openshift-user-workload-monitoring.svc:9095"},
		{name: "fully qualified service", value: "https://alertmanager-user-workload.openshift-user-workload-monitoring.svc.cluster.local:9095/"},
		{name: "relative", value: "/api/v2", wantErr: true},
		{name: "HTTP", value: "http://alertmanager-user-workload.openshift-user-workload-monitoring.svc:9095", wantErr: true},
		{name: "external host", value: "https://" + marker + ".example:9095", wantErr: true},
		{name: "wrong port", value: "https://alertmanager-user-workload.openshift-user-workload-monitoring.svc:9443", wantErr: true},
		{name: "path", value: "https://alertmanager-user-workload.openshift-user-workload-monitoring.svc:9095/" + marker, wantErr: true},
		{name: "credentials and query", value: "https://user:" + marker + "@example.invalid:9095?token=" + marker, wantErr: true},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			o := base
			o.alertmanagerURL = testCase.value
			err := o.validate()
			if (err != nil) != testCase.wantErr {
				t.Fatalf("validate() error = %v, wantErr %t", err, testCase.wantErr)
			}
			if err != nil && (!strings.Contains(err.Error(), "--alertmanager-url") || strings.Contains(err.Error(), marker)) {
				t.Fatalf("unsafe validation error: %v", err)
			}
		})
	}
}
