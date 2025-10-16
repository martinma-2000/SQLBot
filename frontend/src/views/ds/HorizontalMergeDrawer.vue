<script lang="ts" setup>
import { ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { ElMessage, ElMessageBox } from 'element-plus-secondary'
import { datasourceApi } from '@/api/datasource'
import icon_close_outlined from '@/assets/svg/operate/ope-close.svg'
import { encrypted } from './js/aes'

const { t } = useI18n()

const visible = ref(false)
const uploading = ref(false)
const merging = ref(false)
const fileList = ref<any[]>([])

// form fields
const name = ref('')
const description = ref('')

const emits = defineEmits(['refresh'])

const beforeClose = () => {
  if (uploading.value || merging.value) return
  visible.value = false
  reset()
}

const reset = () => {
  fileList.value = []
  name.value = ''
  description.value = ''
}

const open = () => {
  reset()
  visible.value = true
}

const handleExceed = () => {
  ElMessage({ type: 'warning', message: t('ds.form.upload.tip') })
}

const removeFile = (file: any) => {
  fileList.value = fileList.value.filter((f) => f.uid !== file.uid)
}

const onChange = (_file: any, files: any[]) => {
  fileList.value = files
}

const mergeAndCreate = async () => {
  if (!name.value) {
    ElMessage.error(t('ds.form.validate.name_required'))
    return
  }
  if (!fileList.value?.length) {
    ElMessage.error(t('merge.select_multiple_files'))
    return
  }
  try {
    merging.value = true
    const formData = new FormData()
    for (const f of fileList.value) {
      // Element Plus upload stores raw file in file.raw
      formData.append('files', f.raw || f)
    }

    // 调用后端mergeExcelsHorizontally接口，返回JSON数据
    const response: any = await datasourceApi.mergeExcelsHorizontally(formData)

    // 准备创建数据源的配置
    const configuration = {
      filename: response.filename,
      sheets: response.sheets || [],
      mode: 'service_name',
    }

    const tables = Array.isArray(response.sheets)
      ? response.sheets.map((sheet: any) => ({
          table_name: sheet.tableName,
          table_comment: sheet.tableComment || 'Horizontally merged data'
        }))
      : []

    const payload: any = {
      name: name.value,
      description: description.value,
      type: 'excel',
      configuration: encrypted(JSON.stringify(configuration)),
      tables,
    }

    await datasourceApi.add(payload)
    ElMessage.success(t('common.save_success'))
    visible.value = false
    emits('refresh')
  } catch (e: any) {
    const msg = e?.message || e?.data?.message || 'merge failed'
    ElMessageBox.alert(String(msg), t('common.error') || 'Error', { type: 'error' })
  } finally {
    uploading.value = false
    merging.value = false
  }
}

defineExpose({ open })
</script>

<template>
  <el-drawer
    v-model="visible"
    :close-on-click-modal="false"
    destroy-on-close
    size="calc(100% - 100px)"
    modal-class="datasource-drawer-fullscreen"
    direction="btt"
    :before-close="beforeClose"
    :show-close="false"
  >
    <template #header="{ close }">
      <span style="white-space: nowrap">{{ $t('merge.horizontal_merge') }}</span>
      <el-icon class="ed-dialog__headerbtn mrt" style="cursor: pointer" @click="close">
        <icon_close_outlined></icon_close_outlined>
      </el-icon>
    </template>

    <div class="hm-form" v-loading="uploading || merging">
      <el-form label-position="top" label-width="auto">
        <el-form-item :label="t('ds.form.name')" required>
          <el-input v-model="name" clearable :placeholder="$t('ds.form.validate.name_required')" />
        </el-form-item>
        <el-form-item :label="t('ds.form.description')">
          <el-input v-model="description" clearable type="textarea" :rows="2" />
        </el-form-item>

        <el-form-item :label="$t('merge.select_multiple_files')">
          <el-upload
            drag
            multiple
            :auto-upload="false"
            accept=".xlsx,.xls"
            :file-list="fileList"
            :on-change="onChange"
            :on-exceed="handleExceed"
            :on-remove="removeFile"
            :limit="50"
          >
            <i class="el-icon-upload"></i>
            <div class="el-upload__text">{{ $t('merge.drop_or_click') }}</div>
            <template #tip>
              <div class="ed-upload__tip">{{ $t('ds.form.upload.tip') }}</div>
              <div class="merge-requirements">
                <h4>{{ $t('merge.file_requirements') }}</h4>
                <ul>
                  <li>{{ $t('merge.horizontal_requirement1') }}</li>
                  <li>{{ $t('merge.horizontal_requirement2') }}</li>
                  <li>{{ $t('merge.horizontal_requirement3') }}</li>
                  <li>{{ $t('merge.horizontal_requirement4') }}</li>
                  <li>{{ $t('merge.horizontal_requirement5') }}</li>
                </ul>
              </div>
            </template>
          </el-upload>
        </el-form-item>

        <div style="text-align: right; margin-top: 8px">
          <el-button secondary @click="beforeClose">{{ t('common.cancel') }}</el-button>
          <el-button type="primary" @click="mergeAndCreate">{{ $t('merge.merge_and_upload') }}</el-button>
        </div>
      </el-form>
    </div>
  </el-drawer>
</template>

<style lang="less" scoped>
.hm-form {
  width: 800px;
  margin: 0 auto;
  padding: 16px 0 24px 0;

  .merge-requirements {
    margin-top: 16px;
    padding: 16px;
    background: #f5f7fa;
    border: 1px solid #e4e7ed;
    border-radius: 6px;
    box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);

    h4 {
      margin: 0 0 12px 0;
      font-size: 15px;
      font-weight: 600;
      color: #303133;
    }

    ul {
      margin: 0;
      padding-left: 20px;

      li {
        font-size: 14px;
        line-height: 1.8;
        color: #606266;
        margin-bottom: 6px;
        position: relative;

        &:before {
          content: "•";
          color: var(--ed-color-primary);
          font-weight: bold;
          display: inline-block;
          width: 1em;
          margin-left: -1em;
        }
      }
    }
  }
}
</style>

<style lang="less">
.datasource-drawer-fullscreen {
  .ed-drawer__body {
    padding: 0 24px 24px 24px;
  }
}
</style>
