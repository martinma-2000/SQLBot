<template>
  <div class="table-structure-view">
    <div class="header">
      <h3>{{ t('datasource.table_structure') }}</h3>
      <el-button 
        link 
        type="primary" 
        size="small" 
        @click="refreshTableList"
        :loading="loading"
      >
        <el-icon><Refresh /></el-icon>
        {{ t('common.refresh') }}
      </el-button>
    </div>
    
    <div v-if="!currentDatasourceId" class="no-datasource">
      <el-empty :description="t('datasource.no_datasource_selected')" />
    </div>
    
    <div v-else-if="loading" class="loading-container">
      <el-skeleton :rows="5" animated />
    </div>
    
    <div v-else class="content">
      <el-scrollbar class="table-list-scrollbar">
        <div v-if="!tableList.length" class="empty-tables">
          <el-empty :description="t('datasource.no_tables_found')" />
        </div>
        
        <el-collapse v-else v-model="activeTableNames" class="table-collapse">
          <el-collapse-item 
            v-for="table in tableList" 
            :key="table.table_name" 
            :name="table.table_name"
            class="table-item"
          >
            <template #title>
              <div class="table-title">
                <el-icon class="table-icon"><Grid /></el-icon>
                <span class="table-name">{{ table.table_name }}</span>
                <span v-if="table.table_comment || table.custom_comment" class="table-comment">{{ table.table_comment || table.custom_comment }}</span>
              </div>
            </template>
            
            <div class="table-fields">
              <div v-if="loadingFields[table.table_name]" class="field-loading">
                <el-skeleton :rows="3" animated />
              </div>
              
              <div v-else-if="!fieldMap[table.table_name]?.length" class="no-fields">
                {{ t('datasource.no_fields_found') }}
              </div>
              
              <div v-else class="field-list">
                <div 
                  v-for="field in fieldMap[table.table_name]" 
                  :key="field.id"
                  class="field-item"
                >
                  <div class="field-info">
                    <span class="field-name">{{ field.field_name }}</span>
                  </div>
                  <div v-if="field.field_comment || field.custom_comment" class="field-comment">
                    {{ field.custom_comment || field.field_comment }}
                  </div>
                </div>
              </div>
            </div>
          </el-collapse-item>
        </el-collapse>
      </el-scrollbar>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, onMounted } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage } from 'element-plus-secondary'
import { Refresh, Grid } from '@element-plus/icons-vue'
import { datasourceApi } from '@/api/datasource'

const { t } = useI18n()

const props = withDefaults(
  defineProps<{
    currentDatasourceId?: number
  }>(),
  {
    currentDatasourceId: undefined
  }
)

const loading = ref(false)
const tableList = ref<any[]>([])
const fieldMap = ref<Record<string, any[]>>({})
const loadingFields = ref<Record<string, boolean>>({})
const activeTableNames = ref<string[]>([])

// 获取表列表
async function getTableList() {
  console.log('TableStructureView - currentDatasourceId:', props.currentDatasourceId)
  
  if (!props.currentDatasourceId) {
    console.log('TableStructureView - 没有数据源ID，清空表列表')
    tableList.value = []
    return
  }
  
  loading.value = true
  try {
    const res = await datasourceApi.tableList(props.currentDatasourceId)
    tableList.value = res || []
  } catch (error) {
    console.error('获取表列表失败:', error)
    ElMessage.error(t('datasource.get_table_list_failed'))
    tableList.value = []
  } finally {
    loading.value = false
  }
}

// 获取表字段
async function getTableFields(table: any) {
  if (!props.currentDatasourceId || loadingFields.value[table.table_name]) {
    return
  }
  
  loadingFields.value[table.table_name] = true
  try {
    // 使用table.id而不是table_name，因为fieldList API需要table_id
    const res = await datasourceApi.fieldList(table.id)
    fieldMap.value[table.table_name] = res || []
  } catch (error) {
    console.error(`获取表 ${table.table_name} 字段失败:`, error)
    ElMessage.error(t('datasource.get_field_list_failed'))
    fieldMap.value[table.table_name] = []
  } finally {
    loadingFields.value[table.table_name] = false
  }
}

// 刷新表列表
function refreshTableList() {
  fieldMap.value = {}
  loadingFields.value = {}
  activeTableNames.value = []
  getTableList()
}

// 监听表展开状态，懒加载字段信息
watch(activeTableNames, (newActiveNames, oldActiveNames) => {
  const newlyOpened = newActiveNames.filter(name => !oldActiveNames?.includes(name))
  newlyOpened.forEach(tableName => {
    if (!fieldMap.value[tableName]) {
      // 找到对应的table对象
      const table = tableList.value.find(t => t.table_name === tableName)
      if (table) {
        getTableFields(table)
      }
    }
  })
})

// 监听数据源变化
watch(() => props.currentDatasourceId, () => {
  refreshTableList()
}, { immediate: true })

onMounted(() => {
  if (props.currentDatasourceId) {
    getTableList()
  }
})
</script>

<style scoped lang="less">
.table-structure-view {
  height: 100%;
  display: flex;
  flex-direction: column;
  
  .header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px;
    border-bottom: 1px solid #ebeef5;
    
    h3 {
      margin: 0;
      font-size: 16px;
      font-weight: 600;
      color: #303133;
    }
  }
  
  .no-datasource,
  .loading-container {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    padding: 20px;
  }
  
  .content {
    flex: 1;
    overflow: hidden;
    
    .table-list-scrollbar {
      height: 100%;
    }
    
    .empty-tables {
      padding: 40px 20px;
      text-align: center;
    }
  }
  
  .table-collapse {
    border: none;
    
    :deep(.el-collapse-item) {
      border-bottom: 1px solid #ebeef5;
      
      &:last-child {
        border-bottom: none;
      }
      
      .el-collapse-item__header {
        padding: 0 16px;
        height: 48px;
        background: #fafafa;
        border: none;
        
        &:hover {
          background: #f0f0f0;
        }
      }
      
      .el-collapse-item__content {
        padding: 0;
        background: #fff;
      }
    }
  }
  
  .table-title {
    display: flex;
    align-items: center;
    gap: 8px;
    width: 100%;
    
    .table-icon {
      color: #409eff;
    }
    
    .table-name {
      font-weight: 500;
      color: #303133;
    }
    
    .table-comment {
      color: #909399;
      font-size: 12px;
      margin-left: auto;
    }
  }
  
  .table-fields {
    .field-loading {
      padding: 16px;
    }
    
    .no-fields {
      padding: 16px;
      text-align: center;
      color: #909399;
      font-size: 14px;
    }
    
    .field-list {
      .field-item {
        padding: 12px 16px;
        border-bottom: 1px solid #f5f7fa;
        
        &:last-child {
          border-bottom: none;
        }
        
        .field-info {
          display: flex;
          align-items: center;
          gap: 12px;
          
          .field-name {
            font-weight: 500;
            color: #303133;
            min-width: 120px;
          }
          
          .field-type {
            background: #f0f2f5;
            color: #606266;
            padding: 2px 8px;
            border-radius: 4px;
            font-size: 12px;
            font-family: 'Courier New', monospace;
          }
        }
        
        .field-comment {
          margin-top: 4px;
          color: #909399;
          font-size: 12px;
          line-height: 1.4;
        }
      }
    }
  }
}
</style>