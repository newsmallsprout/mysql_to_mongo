<template>
  <el-aside :width="isCollapsed ? '80px' : '260px'" class="sidebar">
    <div class="logo-container">
      <div class="logo-wrapper">
        <img src="@/assets/images/brand.png" alt="Logo" class="logo" />
      </div>
      <span v-if="!isCollapsed" class="title">Shark Platform</span>
    </div>
    
    <div class="menu-wrapper">
      <el-menu
        :default-active="activeMenu"
        class="el-menu-vertical"
        :collapse="isCollapsed"
        background-color="transparent"
        text-color="#94a3b8"
        active-text-color="#ffffff"
        router
        @select="handleMenuSelect"
      >
        <el-menu-item index="/dashboard" v-if="canViewDashboard">
          <el-icon><Odometer /></el-icon>
          <template #title>Dashboard</template>
        </el-menu-item>
        
        <div class="menu-group-title" v-if="!isCollapsed">Data Pipeline</div>
        <el-menu-item index="/tasks" v-if="canViewTasks">
          <el-icon><List /></el-icon>
          <template #title>Sync Tasks</template>
        </el-menu-item>
        <el-menu-item index="/connections" v-if="canViewTasks">
          <el-icon><Link /></el-icon>
          <template #title>Data Sources</template>
        </el-menu-item>
        <el-menu-item index="/database-manager">
          <el-icon><Coin /></el-icon>
          <template #title>Database Manager</template>
        </el-menu-item>

        <div class="menu-group-title" v-if="!isCollapsed">Operations</div>
        <el-menu-item index="/ai-ops">
          <el-icon><Cpu /></el-icon>
          <template #title>Fault Analysis</template>
        </el-menu-item>
        <el-menu-item index="/schedules">
          <el-icon><Calendar /></el-icon>
          <template #title>Schedule Mgmt</template>
        </el-menu-item>
        
        <div class="menu-group-title" v-if="!isCollapsed">System Maintenance</div>
        <el-menu-item index="/logs" v-if="canViewLogs">
          <el-icon><Monitor /></el-icon>
          <template #title>Log Monitor</template>
        </el-menu-item>
        <el-menu-item index="/system" v-if="canViewSystem">
          <el-icon><Setting /></el-icon>
          <template #title>System Inspection</template>
        </el-menu-item>
        <el-menu-item index="/deploy" v-if="canViewDeploy">
          <el-icon><Upload /></el-icon>
          <template #title>Deployment</template>
        </el-menu-item>
        <el-menu-item index="/releases">
          <el-icon><Promotion /></el-icon>
          <template #title>Release Mgmt</template>
        </el-menu-item>
        <el-menu-item index="/permissions" v-if="isAdmin">
          <el-icon><Lock /></el-icon>
          <template #title>Access Control</template>
        </el-menu-item>
      </el-menu>
    </div>

    <div class="sidebar-footer" @click="isCollapsed = !isCollapsed">
      <el-icon :size="20">
        <component :is="isCollapsed ? 'Expand' : 'Fold'" />
      </el-icon>
    </div>
  </el-aside>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { Odometer, List, Link, Monitor, Setting, Upload, Fold, Expand, Lock, Calendar, Cpu, Promotion } from '@element-plus/icons-vue'
import { useSystemStore } from '@/stores/system'

const route = useRoute()
const systemStore = useSystemStore()
const isCollapsed = ref(false)

const isAdmin = computed(() => systemStore.isAdmin)
const canViewDashboard = computed(() => systemStore.isAdmin || systemStore.hasPermission('view_dashboard'))
const canViewTasks = computed(() => systemStore.isAdmin || systemStore.hasPermission('view_tasks'))
const canViewLogs = computed(() => systemStore.isAdmin || systemStore.hasPermission('view_logs'))
const canViewSystem = computed(() => systemStore.isAdmin || systemStore.hasPermission('view_inspection'))
const canViewDeploy = computed(() => systemStore.isAdmin || systemStore.hasPermission('view_deploy'))

const handleMenuSelect = () => {
  // Menu selection handling if needed
}

onMounted(() => {
  systemStore.fetchCurrentUser()
})

const activeMenu = computed(() => {
  const path = route.path
  if (path.startsWith('/tasks')) return '/tasks'
  return path
})
</script>

<style scoped>
.sidebar {
  height: 100vh;
  background-color: var(--sidebar-bg);
  display: flex;
  flex-direction: column;
  box-shadow: 4px 0 24px rgba(0,0,0,0.2);
  z-index: 1000;
  border-right: 1px solid rgba(255,255,255,0.05);
  transition: width 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.logo-container {
  height: 80px;
  display: flex;
  align-items: center;
  padding: 0 24px;
  color: #fff;
  border-bottom: 1px solid rgba(255,255,255,0.05);
}

.logo-wrapper {
  width: 32px;
  height: 32px;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(59, 130, 246, 0.1);
  border-radius: 8px;
}

.logo {
  width: 24px;
  height: 24px;
}

.title {
  margin-left: 16px;
  font-weight: 700;
  font-size: 18px;
  letter-spacing: -0.5px;
  white-space: nowrap;
  background: linear-gradient(to right, #fff, #94a3b8);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
}

.menu-wrapper {
  flex: 1;
  padding: 20px 0;
  overflow-y: auto;
}

.menu-group-title {
  padding: 24px 24px 12px;
  font-size: 11px;
  font-weight: 700;
  color: #475569;
  letter-spacing: 1.2px;
}

:deep(.el-menu) {
  border-right: none;
}

:deep(.el-menu-item) {
  height: 50px;
  margin: 4px 16px;
  border-radius: 12px;
  line-height: 50px;
  font-weight: 500;
  font-size: 14px;
  padding: 0 16px !important;
}

:deep(.el-menu-item.is-active) {
  background: linear-gradient(90deg, #3b82f6, #2563eb) !important;
  color: #ffffff !important;
  box-shadow: 0 4px 12px rgba(59, 130, 246, 0.4);
}

:deep(.el-menu-item.is-active span) {
  color: #ffffff !important;
}

:deep(.el-menu-item.is-active .el-icon) {
  color: #ffffff !important;
}

:deep(.el-menu-item:hover) {
  background-color: rgba(255,255,255,0.05) !important;
  color: #ffffff !important;
}

.sidebar-footer {
  height: 60px;
  display: flex;
  align-items: center;
  justify-content: center;
  color: #64748b;
  cursor: pointer;
  border-top: 1px solid rgba(255,255,255,0.05);
  transition: all 0.2s;
}

.sidebar-footer:hover {
  color: #fff;
  background-color: rgba(255,255,255,0.05);
}
</style>
