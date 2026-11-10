package controller

import (
	"context"
	"fmt"
	"os"

	corev1 "k8s.io/api/core/v1"
	netv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	"github.com/johanjk/imposm-operator/internal/spec"
)

// NetworkPolicyConfig is the chart's networkPolicy settings for the per-sync
// runner policies (--network-policy-config, rendered by the chart).
type NetworkPolicyConfig struct {
	// Ingress denies all ingress to runner pods.
	Ingress bool `json:"ingress"`
	// Egress restricts runner egress to DNS, the sync's database and Redis
	// ports and its upstream ports on public addresses.
	Egress bool `json:"egress"`
	// DNS are the peers DNS queries may go to.
	DNS []netv1.NetworkPolicyPeer `json:"dns"`
	// NonPublicIPv4/IPv6 are excluded from the upstream rule.
	NonPublicIPv4 []string `json:"nonPublicIPv4"`
	NonPublicIPv6 []string `json:"nonPublicIPv6"`
	// Internal are the peers the database and Redis ports may reach (empty:
	// any address).
	Internal []netv1.NetworkPolicyPeer `json:"internal"`
	// ExtraEgress rules are added to every runner policy.
	ExtraEgress []netv1.NetworkPolicyEgressRule `json:"extraEgress"`
}

// LoadNetworkPolicyConfig reads a NetworkPolicyConfig (YAML or JSON).
func LoadNetworkPolicyConfig(path string) (*NetworkPolicyConfig, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c NetworkPolicyConfig
	if err := yaml.UnmarshalStrict(b, &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &c, nil
}

func runnerPolicyName(name string) string { return runnerAccountName(name) }

// runnerNetworkPolicy is the policy for one sync's runner pods, or nil when
// the config restricts neither direction.
func runnerNetworkPolicy(s *Sync, source string, c *NetworkPolicyConfig) *netv1.NetworkPolicy {
	if c == nil || (!c.Ingress && !c.Egress) {
		return nil
	}
	np := &netv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: s.Key.Namespace, Name: runnerPolicyName(s.Key.Name), Labels: labels(s, source)},
		Spec: netv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: selectorLabels(s, source)},
		},
	}
	if c.Ingress {
		np.Spec.PolicyTypes = append(np.Spec.PolicyTypes, netv1.PolicyTypeIngress)
		np.Spec.Ingress = []netv1.NetworkPolicyIngressRule{}
	}
	if c.Egress {
		np.Spec.PolicyTypes = append(np.Spec.PolicyTypes, netv1.PolicyTypeEgress)
		np.Spec.Egress = runnerEgress(spec.Egress(s.Spec), c)
	}
	return np
}

func tcp(ports ...int32) []netv1.NetworkPolicyPort {
	out := make([]netv1.NetworkPolicyPort, 0, len(ports))
	for _, p := range ports {
		out = append(out, netv1.NetworkPolicyPort{Protocol: ptr.To(corev1.ProtocolTCP), Port: ptr.To(intstr.FromInt32(p))})
	}
	return out
}

// runnerEgress: DNS; the database and Redis ports to the configured
// internal peers (by default any pod and any address but link-local); the
// upstream ports to public addresses only; the admin's extra rules.
func runnerEgress(e spec.EgressPorts, c *NetworkPolicyConfig) []netv1.NetworkPolicyEgressRule {
	udp53 := netv1.NetworkPolicyPort{Protocol: ptr.To(corev1.ProtocolUDP), Port: ptr.To(intstr.FromInt32(53))}
	rules := []netv1.NetworkPolicyEgressRule{{To: c.DNS, Ports: append([]netv1.NetworkPolicyPort{udp53}, tcp(53)...)}}
	var internal []int32
	if e.Database > 0 {
		internal = append(internal, e.Database)
	}
	if e.Redis > 0 && e.Redis != e.Database {
		internal = append(internal, e.Redis)
	}
	if len(internal) > 0 {
		rules = append(rules, netv1.NetworkPolicyEgressRule{Ports: tcp(internal...), To: c.Internal})
	}
	rules = append(rules, netv1.NetworkPolicyEgressRule{
		Ports: tcp(e.Upstream...),
		To: []netv1.NetworkPolicyPeer{
			{IPBlock: &netv1.IPBlock{CIDR: "0.0.0.0/0", Except: c.NonPublicIPv4}},
			{IPBlock: &netv1.IPBlock{CIDR: "::/0", Except: c.NonPublicIPv6}},
		},
	})
	return append(rules, c.ExtraEgress...)
}

// ensureNetworkPolicy creates or updates the sync's runner policy, or removes
// one this controller created when policies are off.
func (r *Reconciler) ensureNetworkPolicy(ctx context.Context, s *Sync) error {
	want := runnerNetworkPolicy(s, r.Source.Name(), r.NetworkPolicy)
	if want == nil {
		var cur netv1.NetworkPolicy
		err := r.Client.Get(ctx, client.ObjectKey{Namespace: s.Key.Namespace, Name: runnerPolicyName(s.Key.Name)}, &cur)
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if metav1.IsControlledBy(&cur, s.Owner) {
			return client.IgnoreNotFound(r.Client.Delete(ctx, &cur))
		}
		return nil
	}
	np := &netv1.NetworkPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: want.Namespace, Name: want.Name}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, np, func() error {
		if err := ownedBy(np, s.Owner, "NetworkPolicy"); err != nil {
			return err
		}
		np.Labels = want.Labels
		np.Spec = want.Spec
		return controllerutil.SetControllerReference(s.Owner, np, r.Client.Scheme())
	})
	return err
}
