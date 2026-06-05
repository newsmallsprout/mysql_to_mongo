<template>
  <div class="releases-container">
    <div class="page-header">
      <div class="header-info">
        <h2 class="page-title">发版管理</h2>
        <p class="page-subtitle">ECR 推送 → 自动映射 → Argo CD 目标版本更新 → 人工 Sync</p>
      </div>
      <div class="header-actions">
        <el-tag v-if="!argocdConfigured" type="warning">Argo CD 未配置</el-tag>
        <el-button @click="fetchApps" :loading="loading" :icon="Refresh">刷新</el-button>
      </div>
    </div>

    <el-tabs v-model="activeTab">
      <el-tab-pane label="应用版本" name="apps">
        <el-row :gutter="20">
          <el-col :span="10">
            <el-card shadow="never" class="list-card">
              <template #header>
                <span>应用列表</span>
              </template>
              <div v-loading="loading" class="app-list">
                <div
                  v-for="app in apps"
                  :key="app.id"
                  class="app-item"
                  :class="{ active: selectedAppId === app.id }"
                  @click="selectApp(app)"
                >
                  <div class="app-title">{{ app.display_name }}</div>
                  <div class="app-meta">
                    <code>{{ app.chart_display || app.chart_path }}</code>
                  </div>
                  <div class="app-tags">
                    <el-tag size="small" :type="app.enabled ? 'success' : 'info'">
                      {{ app.enabled ? '启用' : '禁用' }}
                    </el-tag>
                    <el-tag v-if="app.argo_sync_status" size="small" :type="syncTagType(app.argo_sync_status)">
                      {{ app.argo_sync_status }}
                    </el-tag>
                  </div>
                </div>
                <el-empty v-if="!loading && apps.length === 0" description="暂无应用，请先在配置 Tab 添加映射" />
              </div>
            </el-card>
          </el-col>

          <el-col :span="14">
            <el-card shadow="never" v-if="selectedApp" v-loading="historyLoading">
              <template #header>
                <div class="detail-header">
                  <span>{{ selectedApp.display_name }} — 版本历史</span>
                </div>
              </template>

              <el-descriptions :column="2" border size="small" class="app-desc">
                <el-descriptions-item label="ECR Repository">{{ selectedApp.ecr_repository }}</el-descriptions-item>
                <el-descriptions-item label="Argo App">{{ selectedApp.argo_app_name }}</el-descriptions-item>
                <el-descriptions-item label="当前目标版本">
                  <el-tag type="primary">{{ selectedApp.current_tag || '-' }}</el-tag>
                </el-descriptions-item>
                <el-descriptions-item label="Argo 实时版本">
                  {{ selectedApp.argo_live_revision || '-' }}
                </el-descriptions-item>
                <el-descriptions-item label="CHART" :span="2">
                  <code>{{ selectedApp.chart_path }}:{{ selectedApp.current_tag || '?' }}</code>
                </el-descriptions-item>
              </el-descriptions>

              <el-alert
                type="info"
                :closable="false"
                show-icon
                class="sync-hint"
                title="更新 Argo 目标版本后，请前往 Argo CD 手动 Sync 完成部署"
              />

              <el-table :data="records" style="width: 100%; margin-top: 16px" size="small">
                <el-table-column prop="image_tag" label="Tag" min-width="180">
                  <template #default="{ row }">
                    <span>{{ row.image_tag }}</span>
                    <el-tag v-if="row.is_current" size="small" type="success" style="margin-left: 8px">当前</el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="source" label="来源" width="90">
                  <template #default="{ row }">
                    <el-tag size="small" :type="row.source === 'webhook' ? 'primary' : 'warning'">
                      {{ row.source === 'webhook' ? '推送' : '回滚' }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="argo_success" label="Argo" width="80">
                  <template #default="{ row }">
                    <el-tag size="small" :type="row.argo_success ? 'success' : 'danger'">
                      {{ row.argo_success ? '成功' : '失败' }}
                    </el-tag>
                  </template>
                </el-table-column>
                <el-table-column prop="created_at" label="时间" width="170">
                  <template #default="{ row }">{{ formatTime(row.created_at) }}</template>
                </el-table-column>
                <el-table-column label="操作" width="100" fixed="right">
                  <template #default="{ row }">
                    <el-button
                      size="small"
                      type="warning"
                      link
                      :disabled="row.is_current || !row.argo_success"
                      @click="handleRollback(row)"
                    >
                      回滚
                    </el-button>
                  </template>
                </el-table-column>
              </el-table>
            </el-card>
            <el-empty v-else description="请选择左侧应用查看版本历史" />
          </el-col>
        </el-row>
      </el-tab-pane>

      <el-tab-pane label="映射配置" name="config">
        <el-alert
          type="info"
          :closable="false"
          show-icon
          class="config-hint"
          title="首次 ECR 推送会自动创建映射（需配置 ARGOCD_DEFAULT_REPO_URL 等环境变量）；此处可查看或手动调整。"
        />
        <div class="toolbar">
          <el-button type="primary" @click="openConfigDialog()">添加映射</el-button>
        </div>
        <el-table :data="configs" v-loading="configLoading" style="width: 100%">
          <el-table-column prop="display_name" label="展示名" min-width="140" />
          <el-table-column prop="ecr_repository" label="ECR Repository" min-width="160" />
          <el-table-column prop="argo_app_name" label="Argo App" min-width="160" />
          <el-table-column prop="chart_path" label="Chart Path" min-width="200" />
          <el-table-column prop="argo_project" label="Project" width="140" />
          <el-table-column prop="enabled" label="状态" width="80">
            <template #default="{ row }">
              <el-tag size="small" :type="row.enabled ? 'success' : 'info'">
                {{ row.enabled ? '启用' : '禁用' }}
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="操作" width="160" fixed="right">
            <template #default="{ row }">
              <el-button size="small" link type="primary" @click="openConfigDialog(row)">编辑</el-button>
              <el-button size="small" link type="danger" @click="handleDeleteConfig(row.id)">删除</el-button>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>
    </el-tabs>

    <el-dialog v-model="configDialogVisible" :title="configForm.id ? '编辑映射' : '添加映射'" width="560px">
      <el-form :model="configForm" label-position="top">
        <el-form-item label="展示名" required>
          <el-input v-model="configForm.display_name" placeholder="Exchange Activity" />
        </el-form-item>
        <el-row :gutter="16">
          <el-col :span="12">
            <el-form-item label="ECR Repository" required>
              <el-input v-model="configForm.ecr_repository" placeholder="exchange-activity" />
            </el-form-item>
          </el-col>
          <el-col :span="12">
            <el-form-item label="Argo App Name" required>
              <el-input v-model="configForm.argo_app_name" placeholder="EXCHANGE-ACTIVITY" />
            </el-form-item>
          </el-col>
        </el-row>
        <el-form-item label="Argo Project">
          <el-input v-model="configForm.argo_project" placeholder="etz-prd-project" />
        </el-form-item>
        <el-form-item label="OCI Repo URL" required>
          <el-input
            v-model="configForm.repo_url"
            placeholder="197461532043.dkr.ecr.ap-northeast-1.amazonaws.com"
          />
        </el-form-item>
        <el-form-item label="Chart Path" required>
          <el-input v-model="configForm.chart_path" placeholder="helm-main/exchange-activity" />
        </el-form-item>
        <el-form-item label="启用">
          <el-switch v-model="configForm.enabled" />
        </el-form-item>
      </el-form>
      <template #footer>
        <el-button @click="configDialogVisible = false">取消</el-button>
        <el-button type="primary" :loading="configSaving" @click="saveConfig">保存</el-button>
      </template>
    </el-dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, reactive, onMounted } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Refresh } from '@element-plus/icons-vue'
import {
  releasesApi,
  type ReleaseApp,
  type ReleaseRecord,
  type ReleaseAppConfigForm,
} from '@/api/releases'

const activeTab = ref('apps')
const loading = ref(false)
const historyLoading = ref(false)
const configLoading = ref(false)
const configSaving = ref(false)
const argocdConfigured = ref(false)

const apps = ref<ReleaseApp[]>([])
const configs = ref<ReleaseApp[]>([])
const records = ref<ReleaseRecord[]>([])
const selectedAppId = ref<number | null>(null)
const selectedApp = ref<ReleaseApp | null>(null)

const configDialogVisible = ref(false)
const configForm = reactive<ReleaseAppConfigForm & { id?: number }>({
  display_name: '',
  ecr_repository: '',
  argo_app_name: '',
  argo_project: '',
  repo_url: '',
  chart_path: '',
  enabled: true,
})

const formatTime = (iso: string) => {
  if (!iso) return '-'
  return new Date(iso).toLocaleString()
}

const syncTagType = (status: string) => {
  if (status === 'Synced') return 'success'
  if (status === 'OutOfSync') return 'warning'
  return 'info'
}

const fetchApps = async () => {
  loading.value = true
  try {
    const res = await releasesApi.getApps()
    apps.value = res.apps
    argocdConfigured.value = res.argocd_configured
    if (selectedAppId.value) {
      selectedApp.value = apps.value.find((a) => a.id === selectedAppId.value) || null
    }
  } finally {
    loading.value = false
  }
}

const fetchConfigs = async () => {
  configLoading.value = true
  try {
    const res = await releasesApi.getConfigs()
    configs.value = res.configs
  } finally {
    configLoading.value = false
  }
}

const selectApp = async (app: ReleaseApp) => {
  selectedAppId.value = app.id
  selectedApp.value = app
  historyLoading.value = true
  try {
    const res = await releasesApi.getHistory(app.id)
    selectedApp.value = res.app
    records.value = res.records
  } finally {
    historyLoading.value = false
  }
}

const handleRollback = async (record: ReleaseRecord) => {
  if (!selectedAppId.value) return
  try {
    await ElMessageBox.confirm(
      `确认回滚到版本 ${record.image_tag}？\n将更新 Argo CD 目标版本，之后请手动 Sync。`,
      '回滚确认',
      { type: 'warning' }
    )
    const res = await releasesApi.rollback(selectedAppId.value, record.image_tag)
    if (res.status === 'ok') {
      ElMessage.success('回滚成功，请前往 Argo CD 手动 Sync')
    } else {
      ElMessage.warning('Argo CD 更新失败，请查看历史记录详情')
    }
    await selectApp(res.app)
    await fetchApps()
  } catch {
    // cancelled
  }
}

const resetConfigForm = () => {
  configForm.id = undefined
  configForm.display_name = ''
  configForm.ecr_repository = ''
  configForm.argo_app_name = ''
  configForm.argo_project = ''
  configForm.repo_url = ''
  configForm.chart_path = ''
  configForm.enabled = true
}

const openConfigDialog = (row?: ReleaseApp) => {
  resetConfigForm()
  if (row) {
    configForm.id = row.id
    configForm.display_name = row.display_name
    configForm.ecr_repository = row.ecr_repository
    configForm.argo_app_name = row.argo_app_name
    configForm.argo_project = row.argo_project
    configForm.repo_url = row.repo_url
    configForm.chart_path = row.chart_path
    configForm.enabled = row.enabled
  }
  configDialogVisible.value = true
}

const saveConfig = async () => {
  configSaving.value = true
  try {
    const payload: ReleaseAppConfigForm = {
      display_name: configForm.display_name,
      ecr_repository: configForm.ecr_repository,
      argo_app_name: configForm.argo_app_name,
      argo_project: configForm.argo_project,
      repo_url: configForm.repo_url,
      chart_path: configForm.chart_path,
      enabled: configForm.enabled,
    }
    if (configForm.id) {
      await releasesApi.updateConfig(configForm.id, payload)
      ElMessage.success('更新成功')
    } else {
      await releasesApi.createConfig(payload)
      ElMessage.success('创建成功')
    }
    configDialogVisible.value = false
    await fetchConfigs()
    await fetchApps()
  } finally {
    configSaving.value = false
  }
}

const handleDeleteConfig = async (id: number) => {
  try {
    await ElMessageBox.confirm('确认删除该映射配置？', '删除确认', { type: 'warning' })
    await releasesApi.deleteConfig(id)
    ElMessage.success('已删除')
    if (selectedAppId.value === id) {
      selectedAppId.value = null
      selectedApp.value = null
      records.value = []
    }
    await fetchConfigs()
    await fetchApps()
  } catch {
    // cancelled
  }
}

onMounted(async () => {
  await Promise.all([fetchApps(), fetchConfigs()])
})
</script>

<style scoped>
.releases-container {
  padding: 0 4px;
}

.page-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 24px;
}

.page-title {
  margin: 0 0 4px;
  font-size: 24px;
  font-weight: 700;
}

.page-subtitle {
  margin: 0;
  color: #64748b;
  font-size: 14px;
}

.header-actions {
  display: flex;
  gap: 12px;
  align-items: center;
}

.list-card {
  min-height: 480px;
}

.app-list {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.app-item {
  padding: 12px 14px;
  border-radius: 10px;
  border: 1px solid #e2e8f0;
  cursor: pointer;
  transition: all 0.2s;
}

.app-item:hover {
  border-color: #93c5fd;
  background: #f8fafc;
}

.app-item.active {
  border-color: #3b82f6;
  background: #eff6ff;
}

.app-title {
  font-weight: 600;
  margin-bottom: 4px;
}

.app-meta code {
  font-size: 12px;
  color: #475569;
}

.app-tags {
  margin-top: 8px;
  display: flex;
  gap: 6px;
}

.detail-header {
  font-weight: 600;
}

.app-desc {
  margin-bottom: 12px;
}

.sync-hint {
  margin-top: 12px;
}

.toolbar {
  margin-bottom: 16px;
}

.config-hint {
  margin-bottom: 16px;
}
</style>
