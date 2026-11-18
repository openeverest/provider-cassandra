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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"

	backupv1alpha1 "github.com/openeverest/openeverest/v2/api/backup/v1alpha1"
	s3class "github.com/openeverest/openeverest/v2/backupstorageclasses/s3"
	"github.com/openeverest/openeverest/v2/provider-runtime/controller"

	"github.com/openeverest/provider-cassandra/definition/backupclasses"
)

func TestShippedBackupClasses(t *testing.T) {
	t.Parallel()

	p := New()
	classes := p.ShippedBackupClasses()
	require.Len(t, classes, 1)
	class := classes[0]
	assert.Equal(t, backupClassName, class.Name)
	assert.Equal(t, p.Name(), class.Spec.ExecutorProvider())

	entry := class.Spec.FindBackupStorageClass(s3class.ClassName)
	require.NotNil(t, entry)
	require.NotNil(t, entry.ParametersSchema)
	assert.Contains(t, entry.ParametersSchema.OpenAPIV3Schema.Properties["implementation"].Enum,
		apiextensionsv1.JSON{Raw: []byte(`"gcs"`)}, "Medusa writes to GCS as s3_compatible",
	)
	assert.Equal(t, &backupv1alpha1.BackupStorageTLSSupport{
		CustomCA:           backupv1alpha1.TLSOptionNone,
		InsecureSkipVerify: backupv1alpha1.TLSOptionSupported,
	}, entry.TLS)
	assert.Equal(t, controller.TLSSupport{CustomCA: false, SkipVerify: true}, backupclasses.TLSSupport(backupClassName, s3class.ClassName))
}
