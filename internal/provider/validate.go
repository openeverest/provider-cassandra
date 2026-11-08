// Copyright (C) 2026 The OpenEverest Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package provider

import (
	"k8s.io/apimachinery/pkg/util/validation/field"

	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/internal/common"
)

// Validate checks the rules about the Instance alone. The runtime runs it once
// the Instance matches the topologies, components, versions and parameter
// schemas the Provider declares.
func (p *Provider) Validate(in controller.ValidationInput) controller.ValidationResult {
	enginePath := field.NewPath("spec", "components").Key(common.ComponentEngine)
	engine, ok := in.Instance.Spec.Components[common.ComponentEngine]
	if !ok {
		return controller.ValidationResult{Errors: field.ErrorList{field.Required(enginePath, "")}}
	}

	var errs field.ErrorList
	if engine.Replicas != nil && *engine.Replicas < 1 {
		errs = append(errs, field.Invalid(enginePath.Child("replicas"), *engine.Replicas, "must be at least 1"))
	}
	_, jvmErrs := resolveJvmOptions(engine)
	errs = append(errs, jvmErrs...)
	errs = append(errs, validateScheduling(enginePath.Child("schedulingPolicy"), engine.SchedulingPolicy)...)
	return controller.ValidationResult{Errors: errs}
}
