package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Group is the API group, also used as label/annotation prefix.
const Group = "imposm.johanjk.github.io"

// Labels and annotations.
const (
	// LabelSync marks a ConfigMap that carries an ImposmSyncSpec (key SpecKey).
	LabelSync = Group + "/sync"
	// LabelName is set on every object created for a sync.
	LabelName = Group + "/name"
	// LabelSource tells which front-end owns generated objects (crd|configmap).
	LabelSource = Group + "/source"
	// AnnotationAction requests a one-off action: sync, deploy, revert-deploy,
	// remove-backup, reimport. The controller clears it once the Job exists.
	AnnotationAction = Group + "/action"
	// AnnotationSpecHash on runner pods: hash of the rendered spec.
	AnnotationSpecHash = Group + "/spec-hash"
	// SpecKey is the ConfigMap key holding the spec YAML.
	SpecKey = "spec.yaml"
)

var (
	GroupVersion  = schema.GroupVersion{Group: Group, Version: "v1alpha1"}
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &ImposmSync{}, &ImposmSyncList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
