import request from '@/utils/request'

export interface ReleaseApp {
  id: number
  display_name: string
  ecr_repository: string
  argo_app_name: string
  argo_project: string
  repo_url: string
  chart_path: string
  enabled: boolean
  current_tag: string
  chart_display: string
  updated_at: string | null
  argo_live_revision?: string
  argo_sync_status?: string
  argo_health?: string
  argo_error?: string
}

export interface ReleaseRecord {
  id: number
  image_tag: string
  image_digest: string
  source: 'webhook' | 'rollback'
  argo_success: boolean
  argo_error: string
  is_current: boolean
  operator: string | null
  created_at: string
}

export interface ReleaseAppConfigForm {
  display_name: string
  ecr_repository: string
  argo_app_name: string
  argo_project: string
  repo_url: string
  chart_path: string
  enabled: boolean
}

export const releasesApi = {
  getApps: () => request.get<{ apps: ReleaseApp[]; argocd_configured: boolean }>('/releases/apps'),
  getHistory: (appId: number) =>
    request.get<{ app: ReleaseApp; records: ReleaseRecord[] }>(`/releases/apps/${appId}/history`),
  rollback: (appId: number, image_tag: string) =>
    request.post<{ status: string; record: ReleaseRecord; app: ReleaseApp }>(
      `/releases/apps/${appId}/rollback`,
      { image_tag }
    ),
  getConfigs: () => request.get<{ configs: ReleaseApp[] }>('/releases/config'),
  createConfig: (data: ReleaseAppConfigForm) =>
    request.post<{ config: ReleaseApp }>('/releases/config', data),
  updateConfig: (id: number, data: Partial<ReleaseAppConfigForm>) =>
    request.put<{ config: ReleaseApp }>(`/releases/config/${id}`, data),
  deleteConfig: (id: number) => request.delete(`/releases/config/${id}`),
}
