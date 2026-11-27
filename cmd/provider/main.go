// package main implements the provider-cassandra provider.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/openeverest/openeverest/v2/provider-runtime/preflight"
	"github.com/openeverest/openeverest/v2/provider-runtime/reconciler"

	"github.com/openeverest/provider-cassandra/internal/operatorcrds"
	"github.com/openeverest/provider-cassandra/internal/provider"
)

// main is the entry point for the provider.
func main() {
	var serverPort int
	var metricsBindAddress string
	var preflightSpec string
	var applyOperatorCRDs bool
	var admission bool

	flag.IntVar(&serverPort, "server-port", 8082, "The port for the provider HTTP server.")
	flag.StringVar(&metricsBindAddress, "metrics-bind-address", ":8081", "The address the metrics endpoint binds to. Use 0 to disable.")
	flag.StringVar(&preflightSpec, "preflight-spec", "", "Run the upgrade preflight against the target Provider spec at this path and exit. Used by the chart's pre-upgrade hook.")
	flag.BoolVar(&applyOperatorCRDs, "apply-operator-crds", false, "With --preflight-spec, apply the bundled operator's CRDs once the preflight passes. Used by the chart's pre-upgrade hook.")
	flag.BoolVar(&admission, "admission", true, "Serve the Instance admission webhook, with the serving certificate the chart mounts.")
	flag.Parse()

	l := ctrl.Log.WithName("setup")
	ctx := ctrl.SetupSignalHandler()

	if preflightSpec != "" {
		if err := preflight.Run(ctx, provider.New(), preflight.Options{TargetSpecFile: preflightSpec}); err != nil {
			if !errors.Is(err, preflight.ErrUpgradeBlocked) {
				// The controller-runtime logger is not initialized on this
				// path; stderr is the pod log the hook preserves.
				fmt.Fprintln(os.Stderr, "upgrade preflight could not run:", err)
			}
			os.Exit(1)
		}
		if applyOperatorCRDs {
			if err := applyBundledOperatorCRDs(ctx); err != nil {
				fmt.Fprintln(os.Stderr, "upgrade preflight could not apply the operator CRDs:", err)
				os.Exit(1)
			}
		}
		return
	}

	// The logger is set up by reconciler.New, so errors before it go to stderr.
	cfg, err := ctrl.GetConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "unable to load kubeconfig:", err)
		os.Exit(1)
	}
	openShift, err := provider.DetectOpenShift(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "unable to detect the cluster platform:", err)
		os.Exit(1)
	}
	p := provider.New(provider.WithOpenShift(openShift))

	opts := []reconciler.ReconcilerOption{
		// Serve the health and readiness probes.
		reconciler.WithServer(reconciler.ServerConfig{Port: serverPort}),
	}

	if metricsBindAddress != "0" {
		opts = append(opts, reconciler.WithMetrics(metricsBindAddress))
	}
	if admission {
		opts = append(opts, reconciler.WithAdmission())
	}

	r, err := reconciler.New(ctx, p, opts...)
	if err != nil {
		l.Error(err, "unable to create reconciler")
		os.Exit(1)
	}
	l.Info("detected cluster platform", "openShift", openShift)

	if err := r.Start(ctx); err != nil {
		l.Error(err, "unable to start reconciler")
		os.Exit(1)
	}
}

func applyBundledOperatorCRDs(ctx context.Context) error {
	cfg, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("get kubeconfig: %w", err)
	}
	c, err := client.New(cfg, client.Options{})
	if err != nil {
		return fmt.Errorf("create client: %w", err)
	}
	if err := operatorcrds.Apply(ctx, c); err != nil {
		return err
	}
	fmt.Println("operator CRDs applied")
	return nil
}
