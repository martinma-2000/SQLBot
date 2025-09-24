<script lang="ts" setup>
import { ref, reactive, onMounted, computed, watch, nextTick, onBeforeUnmount } from 'vue'
import { useI18n } from 'vue-i18n'
import { datasourceApi } from '@/api/datasource'
import icon_upload_outlined from '@/assets/svg/icon_upload_outlined.svg'
import icon_searchOutline_outlined from '@/assets/svg/icon_search-outline_outlined.svg'
import { encrypted, decrypted } from './js/aes'
import { ElMessage } from 'element-plus-secondary'
import type { FormInstance, FormRules } from 'element-plus-secondary'
import icon_form_outlined from '@/assets/svg/icon_form_outlined.svg'
import FixedSizeList from 'element-plus-secondary/es/components/virtual-list/src/components/fixed-size-list.mjs'
import { debounce } from 'lodash-es'
import { Plus } from '@element-plus/icons-vue'
import { haveSchema } from '@/views/ds/js/ds-type'
import { setSize } from '@/utils/utils'
import EmptyBackground from '@/views/dashboard/common/EmptyBackground.vue'
import icon_fileExcel_colorful from '@/assets/datasource/icon_excel.png'
import IconOpeDelete from '@/assets/svg/icon_delete.svg'

const props = withDefaults(
  defineProps<{
    activeName: string
    activeType: string
    activeStep: number
    isDataTable: boolean
    concatenateMode?: boolean
  }>(),
  {
    activeName: '',
    activeType: '',
    activeStep: 0,
    isDataTable: false,
    concatenateMode: false,
  }
)

const dsFormRef = ref<FormInstance>()
const emit = defineEmits(['refresh', 'changeActiveStep', 'close'])
const isCreate = ref(true)
const isEditTable = ref(false)
const checkList = ref<any>([])
const tableList = ref<any>([])
const excelUploadSuccess = ref(false)
const tableListLoading = ref(false)
const checkLoading = ref(false)
const dialogTitle = ref('')
const getUploadURL = import.meta.env.VITE_API_BASE_URL + '/datasource/uploadExcel'
const saveLoading = ref<boolean>(false)
const uploadLoading = ref(false)
const { t } = useI18n()
const schemaList = ref<any>([])

// 纵向合并模式相关变量
const multipleFiles = ref<File[]>([])

// 格式化文件大小
const formatFileSize = (size: number) => {
  return setSize(size)
}

// 处理多文件选择
const handleMultipleFileChange = (_file: any, fileList: any[]) => {
  const newFiles = fileList.map(item => item.raw).filter(Boolean)
  multipleFiles.value = newFiles
}

// 移除文件
const removeFile = (index: number) => {
  multipleFiles.value.splice(index, 1)
}

const rules = reactive<FormRules>({
  name: [
    {
      required: true,
      message: t('datasource.please_enter') + t('common.empty') + t('ds.form.name'),
      trigger: 'blur',
    },
    { min: 1, max: 50, message: t('ds.form.validate.name_length'), trigger: 'blur' },
  ],
  type: [
    {
      required: true,
      message: t('datasource.Please_select') + t('common.empty') + t('ds.type'),
      trigger: 'change',
    },
  ],
  host: [
    {
      required: true,
      message: t('datasource.please_enter') + t('common.empty') + t('ds.form.host'),
      trigger: 'blur',
    },
  ],
  port: [
    {
      required: true,
      message: t('datasource.please_enter') + t('common.empty') + t('ds.form.port'),
      trigger: 'blur',
    },
  ],
  database: [
    {
      required: true,
      message: t('datasource.please_enter') + t('common.empty') + t('ds.form.database'),
      trigger: 'blur',
    },
  ],
  mode: [{ required: true, message: 'Please choose mode', trigger: 'change' }],
  sheets: [
    { 
      required: true, 
      message: t('user.upload_file'), 
      trigger: 'change',
      validator: (_rule: any, value: any, callback: any) => {
        // 纵向合并模式下跳过sheets验证
        if (props.concatenateMode) {
          callback()
          return
        }
        // 普通模式下进行正常验证
        if (!value || value.length === 0) {
          callback(new Error(t('user.upload_file')))
        } else {
          callback()
        }
      }
    }
  ],
  dbSchema: [
    {
      required: true,
      message: t('datasource.please_enter') + t('common.empty') + 'Schema',
      trigger: 'blur',
    },
  ],
})

const dialogVisible = ref<boolean>(false)
const form = ref<any>({
  name: '',
  description: '',
  type: props.activeType,
  configuration: '',
  driver: '',
  host: '',
  port: 0,
  username: '',
  password: '',
  database: '',
  extraJdbc: '',
  dbSchema: '',
  filename: '',
  sheets: [],
  mode: 'service_name',
  timeout: 30,
})

const close = () => {
  dialogVisible.value = false
  isCreate.value = true
  emit('changeActiveStep', 0)
  emit('close')
  isEditTable.value = false
  checkList.value = []
  tableList.value = []
  excelUploadSuccess.value = false
  saveLoading.value = false
}

const initForm = (item: any, editTable: boolean = false) => {
  isEditTable.value = false
  keywords.value = ''
  dsFormRef.value!.clearValidate()
  if (item) {
    dialogTitle.value = editTable ? t('ds.form.title.choose_tables') : t('ds.form.title.edit')
    isCreate.value = false
    form.value.id = item.id
    form.value.name = item.name
    form.value.description = item.description
    form.value.type = item.type
    form.value.configuration = item.configuration
    if (item.configuration) {
      const configuration = JSON.parse(decrypted(item.configuration))
      form.value.host = configuration.host
      form.value.port = configuration.port
      form.value.username = configuration.username
      form.value.password = configuration.password
      form.value.database = configuration.database
      form.value.extraJdbc = configuration.extraJdbc
      form.value.dbSchema = configuration.dbSchema
      form.value.filename = configuration.filename
      form.value.sheets = configuration.sheets
      form.value.mode = configuration.mode
      form.value.timeout = configuration.timeout ? configuration.timeout : 30
    }

    if (editTable) {
      dialogTitle.value = t('ds.form.choose_tables')
      emit('changeActiveStep', 2)
      isEditTable.value = true
      isCreate.value = false
      // request tables and check tables

      datasourceApi.tableList(item.id).then((res: any) => {
        checkList.value = res.map((ele: any) => {
          return ele.table_name
        })
        if (item.type === 'excel') {
          tableList.value = form.value.sheets
          nextTick(() => {
            handleCheckedTablesChange([...checkList.value])
          })
        } else {
          tableListLoading.value = true
          const requestObj = buildConf()
          datasourceApi
            .getTablesByConf(requestObj)
            .then((table) => {
              tableList.value = table
              checkList.value = checkList.value.filter((ele: string) => {
                return table
                  .map((ele: any) => {
                    return ele.tableName
                  })
                  .includes(ele)
              })
              nextTick(() => {
                handleCheckedTablesChange([...checkList.value])
              })
            })
            .finally(() => {
              tableListLoading.value = false
            })
        }
      })
    }
  } else {
    dialogTitle.value = t('ds.form.title.add')
    isCreate.value = true
    isEditTable.value = false
    checkList.value = []
    tableList.value = []
    form.value = {
      name: '',
      description: '',
      type: 'mysql',
      configuration: '',
      driver: '',
      host: '',
      port: 0,
      username: '',
      password: '',
      database: '',
      extraJdbc: '',
      dbSchema: '',
      filename: '',
      sheets: [],
      mode: 'service_name',
      timeout: 30,
    }
  }
  dialogVisible.value = true
}

const save = async (formEl: FormInstance | undefined) => {
  if (!formEl) return
  
  // 纵向合并模式的处理逻辑
  if (props.concatenateMode) {
    if (multipleFiles.value.length < 2) {
      ElMessage({
        message: '请至少选择两个文件进行合并',
        type: 'warning',
        showClose: true,
      })
      return
    }
    
    // 验证表单基本信息
    await formEl.validate(async (valid) => {
      if (valid) {
        saveLoading.value = true
        
        try {
          // 创建FormData
          const formData = new FormData()
          multipleFiles.value.forEach((file) => {
            formData.append('files', file)
          })
          formData.append('separator', '_')
          formData.append('primary_key_col', '0')
          
          // 调用纵向合并接口
          const response = await datasourceApi.concatenateExcels(formData)
          
          // 设置合并后的文件信息
          form.value.filename = response.filename
          form.value.sheets = response.sheets
          
          // 创建数据源
          const list = response.sheets.map((sheet: any) => ({
            table_name: sheet.tableName,
            table_comment: sheet.tableComment
          }))
          
          const requestObj = buildConf()
          requestObj.tables = list
          
          await datasourceApi.add(requestObj)
          
          ElMessage({
            message: '纵向合并数据源创建成功！',
            type: 'success',
            showClose: true,
          })
          
          // 触发刷新事件，这会关闭弹出页面并刷新数据源列表
          emit('refresh')
        } catch (error) {
          console.error('创建纵向合并数据源失败:', error)
          ElMessage({
            message: '创建纵向合并数据源失败，请检查文件格式',
            type: 'error',
            showClose: true,
          })
        } finally {
          saveLoading.value = false
        }
      }
    })
    return
  }
  
  // 原有的保存逻辑
  await formEl.validate(async (valid) => {
    if (valid) {
      const list = tableList.value
        .filter((ele: any) => {
          return checkTableList.value.includes(ele.tableName)
        })
        .map((ele: any) => {
          return { table_name: ele.tableName, table_comment: ele.tableComment }
        })

      if (checkTableList.value.length > 30) {
        const excessive = await ElMessageBox.confirm(t('common.excessive_tables_selected'), {
          tip: t('common.to_continue_saving', { msg: checkTableList.value.length }),
          confirmButtonText: t('common.save'),
          cancelButtonText: t('common.cancel'),
          confirmButtonType: 'primary',
          type: 'warning',
          customClass: 'confirm-with_icon',
          autofocus: false,
        })

        if (excessive !== 'confirm') return
      }
      saveLoading.value = true

      const requestObj = buildConf()
      if (form.value.id) {
        if (!isEditTable.value) {
          // only update datasource config info
          datasourceApi
            .update(requestObj)
            .then(() => {
              close()
              emit('refresh')
            })
            .finally(() => {
              saveLoading.value = false
            })
        } else {
          // save table and field
          datasourceApi
            .chooseTables(form.value.id, list)
            .then(() => {
              close()
              emit('refresh')
            })
            .finally(() => {
              saveLoading.value = false
            })
        }
      } else {
        requestObj.tables = list
        datasourceApi
          .add(requestObj)
          .then(() => {
            close()
            emit('refresh')
          })
          .finally(() => {
            saveLoading.value = false
          })
      }
    }
  })
}

const buildConf = () => {
  form.value.configuration = encrypted(
    JSON.stringify({
      host: form.value.host,
      port: form.value.port,
      username: form.value.username,
      password: form.value.password,
      database: form.value.database,
      extraJdbc: form.value.extraJdbc,
      dbSchema: form.value.dbSchema,
      filename: form.value.filename,
      sheets: form.value.sheets,
      mode: form.value.mode,
      timeout: form.value.timeout,
    })
  )
  const obj = JSON.parse(JSON.stringify(form.value))
  delete obj.driver
  delete obj.host
  delete obj.port
  delete obj.username
  delete obj.password
  delete obj.database
  delete obj.extraJdbc
  delete obj.dbSchema
  delete obj.filename
  delete obj.sheets
  delete obj.mode
  delete obj.timeout
  return obj
}

const check = () => {
  const requestObj = buildConf()
  datasourceApi.check(requestObj).then((res: any) => {
    if (res) {
      ElMessage({
        message: t('ds.form.connect.success'),
        type: 'success',
        showClose: true,
      })
    } else {
      ElMessage({
        message: t('ds.form.connect.failed'),
        type: 'error',
        showClose: true,
      })
    }
  })
}

const getSchema = () => {
  schemaList.value = []
  const requestObj = buildConf()
  datasourceApi.getSchema(requestObj).then((res: any) => {
    for (let item of res) {
      schemaList.value.push({ label: item, value: item })
    }
  })
}

onBeforeUnmount(() => (saveLoading.value = false))

const next = debounce(async (formEl: FormInstance | undefined) => {
  if (!formEl) return
  
  // 纵向合并模式：跳过表单验证，直接检查文件
  if (props.concatenateMode) {
    if (multipleFiles.value.length >= 2) {
      // 检查数据源名称是否填写
      if (!form.value.name || form.value.name.trim() === '') {
        ElMessage({
          message: '请输入数据源名称',
          type: 'warning',
          showClose: true,
        })
        return
      }
      
      // 调用后端API进行文件合并和数据表创建
      const formData = new FormData()
      multipleFiles.value.forEach(file => {
        formData.append('files', file)
      })
      formData.append('separator', '_')
      formData.append('primary_key_col', '0')
      
      try {
        const response = await datasourceApi.concatenateExcels(formData)
        // 设置合并后的文件信息
        form.value.filename = response.filename
        form.value.sheets = response.sheets
        tableList.value = response.sheets
        excelUploadSuccess.value = true
        
        // 纵向合并模式：由于合并后只有一个sheet，直接保存数据源，跳过数据表选择步骤
        checkList.value = response.sheets.map((sheet: any) => sheet.tableName)
        await save(dsFormRef.value)
        return
      } catch (error) {
        ElMessage({
          message: '文件合并失败，请检查文件格式和内容',
          type: 'error',
          showClose: true,
        })
        console.error('文件合并错误:', error)
      }
    } else {
      ElMessage({
        message: '请至少选择两个文件进行合并',
        type: 'warning',
        showClose: true,
      })
    }
    return
  }
  
  // 普通模式：进行表单验证
  await formEl.validate((valid) => {
    if (valid) {
      if (form.value.type === 'excel') {
        // 普通Excel模式：检查上传成功状态
        if (excelUploadSuccess.value) {
          emit('changeActiveStep', props.activeStep + 1)
        }
      } else {
        if (checkLoading.value) return
        // check status if success do next
        const requestObj = buildConf()
        checkLoading.value = true
        datasourceApi
          .check(requestObj)
          .then((res: boolean) => {
            if (res) {
              emit('changeActiveStep', props.activeStep + 1)
              // request tables
              datasourceApi.getTablesByConf(requestObj).then((res: any) => {
                tableList.value = res
              })
            } else {
              ElMessage({
                message: t('ds.form.connect.failed'),
                type: 'error',
                showClose: true,
              })
            }
          })
          .finally(() => {
            checkLoading.value = false
          })
      }
    }
  })
}, 300)

const preview = debounce(() => {
  emit('changeActiveStep', props.activeStep - 1)
}, 200)

const beforeUpload = (rawFile: any) => {
  setFile(rawFile)
  if (rawFile.size / 1024 / 1024 > 50) {
    ElMessage.error(t('common.not_exceed_50mb'))
    return false
  }
  uploadLoading.value = true
  return true
}

const onSuccess = (response: any) => {
  form.value.filename = response.data.filename
  form.value.sheets = response.data.sheets
  tableList.value = response.data.sheets
  excelUploadSuccess.value = true
  uploadLoading.value = false
}

const onError = () => {
  uploadLoading.value = false
}

onMounted(() => {
  setTimeout(() => {
    dsFormRef.value!.clearValidate()
  }, 100)
})

const keywords = ref('')
const tableListWithSearch = computed(() => {
  if (!keywords.value) return tableList.value
  return tableList.value.filter((ele: any) =>
    ele.tableName.toLowerCase().includes(keywords.value.toLowerCase())
  )
})

watch(keywords, () => {
  const tableNameArr = tableListWithSearch.value.map((ele: any) => ele.tableName)
  checkList.value = checkTableList.value.filter((ele) => tableNameArr.includes(ele))
  const checkedCount = checkList.value.length
  checkAll.value = checkedCount === tableListWithSearch.value.length
  isIndeterminate.value = checkedCount > 0 && checkedCount < tableListWithSearch.value.length
})

watch(
  () => props.activeType,
  (val) => {
    form.value.type = val
    // 纵向合并模式下，设置sheets为非空数组以通过验证
    if (props.concatenateMode && val === 'excel') {
      form.value.sheets = ['placeholder'] // 设置占位符以通过验证
    }
  },
  { immediate: true }  // 立即执行一次
)

// 监听concatenateMode变化
watch(
  () => props.concatenateMode,
  (isConcatenate) => {
    if (isConcatenate && form.value.type === 'excel') {
      form.value.sheets = ['placeholder'] // 设置占位符以通过验证
    } else if (!isConcatenate) {
      form.value.sheets = []
    }
  },
  { immediate: true }
)
const fileSize = ref('-')
const clearFile = () => {
  fileSize.value = ''
  form.value.filename = ''
  // 纵向合并模式下保持占位符，避免验证失败
  if (props.concatenateMode && form.value.type === 'excel') {
    form.value.sheets = ['placeholder']
  } else {
    form.value.sheets = []
  }
  tableList.value = []
}

const setFile = (file: any) => {
  fileSize.value = setSize(file.size)
}

const checkAll = ref(false)
const isIndeterminate = ref(false)
const checkTableList = ref([] as any[])

const handleCheckAllChange = (val: any) => {
  checkList.value = val
    ? [
        ...new Set([
          ...tableListWithSearch.value.map((ele: any) => ele.tableName),
          ...checkList.value,
        ]),
      ]
    : []
  isIndeterminate.value = false
  const tableNameArr = tableListWithSearch.value.map((ele: any) => ele.tableName)
  checkTableList.value = val
    ? [...new Set([...tableNameArr, ...checkTableList.value])]
    : checkTableList.value.filter((ele) => !tableNameArr.includes(ele))
}

const handleCheckedTablesChange = (value: any[]) => {
  const checkedCount = value.length
  checkAll.value = checkedCount === tableListWithSearch.value.length
  isIndeterminate.value = checkedCount > 0 && checkedCount < tableListWithSearch.value.length
  const tableNameArr = tableListWithSearch.value.map((ele: any) => ele.tableName)
  checkTableList.value = [
    ...new Set([...checkTableList.value.filter((ele) => !tableNameArr.includes(ele)), ...value]),
  ]
}

const tableListSave = () => {
  save(dsFormRef.value)
}

defineExpose({
  initForm,
  tableListSave,
})
</script>

<template>
  <div
    v-loading="uploadLoading || saveLoading || checkLoading"
    class="model-form"
    :class="(!isCreate || activeStep === 2) && 'edit-form'"
  >
    <div v-if="isCreate && activeStep !== 2" class="model-name">
      {{ activeName }}
      <span v-if="form.type !== 'excel'" style="margin-left: 8px; color: #8f959e; font-size: 12px">
        <span>{{ t('ds.form.support_version') }}:&nbsp;</span>
        <span v-if="form.type === 'sqlServer'">2012+</span>
        <span v-else-if="form.type === 'oracle'">12+</span>
        <span v-else-if="form.type === 'mysql'">5.6+</span>
        <span v-else-if="form.type === 'pg'">9.6+</span>
        <span v-else-if="form.type === 'es'">7+</span>
      </span>
    </div>
    <div class="form-content">
      <el-form
        v-show="activeStep === 1"
        ref="dsFormRef"
        :model="form"
        label-position="top"
        label-width="auto"
        :rules="rules"
        @submit.prevent
      >
        <div v-if="form.type === 'excel' || props.concatenateMode">
          <el-form-item prop="sheets" :label="t('ds.form.file')">
            <!-- 纵向合并模式：显示多个文件 -->
            <div v-if="props.concatenateMode && multipleFiles.length > 0">
              <div v-for="(file, index) in multipleFiles" :key="index" class="pdf-card" style="margin-bottom: 8px;">
                <img :src="icon_fileExcel_colorful" width="40px" height="40px" />
                <div class="file-name">
                  <div class="name">{{ file.name }}</div>
                  <div class="size">{{ file.name.split('.')[1] }} - {{ formatFileSize(file.size) }}</div>
                </div>
                <el-icon class="action-btn" size="16" @click="removeFile(index)">
                  <IconOpeDelete></IconOpeDelete>
                </el-icon>
              </div>
            </div>
            <!-- 普通模式：显示单个文件 -->
            <div v-else-if="form.filename && !props.concatenateMode" class="pdf-card">
              <img :src="icon_fileExcel_colorful" width="40px" height="40px" />
              <div class="file-name">
                <div class="name">{{ form.filename }}</div>
                <div class="size">{{ form.filename.split('.')[1] }} - {{ fileSize }}</div>
              </div>
              <el-icon v-if="!form.id" class="action-btn" size="16" @click="clearFile">
                <IconOpeDelete></IconOpeDelete>
              </el-icon>
            </div>
            <!-- 纵向合并模式的上传按钮 -->
            <el-upload
              v-if="props.concatenateMode"
              class="upload-user"
              accept=".xlsx,.xls,.csv"
              :auto-upload="false"
              :multiple="true"
              :on-change="handleMultipleFileChange"
              :show-file-list="false"
            >
              <el-button secondary>
                <el-icon size="16" style="margin-right: 4px">
                  <icon_upload_outlined></icon_upload_outlined>
                </el-icon>
                {{ multipleFiles.length > 0 ? '添加更多文件' : '选择多个文件' }}
              </el-button>
            </el-upload>
            <!-- 普通模式的上传按钮 -->
            <el-upload
              v-else-if="form.filename && !form.id"
              class="upload-user"
              accept=".xlsx,.xls,.csv"
              :action="getUploadURL"
              :before-upload="beforeUpload"
              :on-error="onError"
              :on-success="onSuccess"
              :show-file-list="false"
              :file-list="form.sheets"
            >
              <el-button text style="line-height: 22px; height: 22px">
                {{ $t('common.re_upload') }}
              </el-button>
            </el-upload>
            <el-upload
              v-else-if="!form.id && !props.concatenateMode"
              class="upload-user"
              accept=".xlsx,.xls,.csv"
              :action="getUploadURL"
              :before-upload="beforeUpload"
              :on-success="onSuccess"
              :on-error="onError"
              :show-file-list="false"
              :file-list="form.sheets"
            >
              <el-button secondary>
                <el-icon size="16" style="margin-right: 4px">
                  <icon_upload_outlined></icon_upload_outlined>
                </el-icon>
                {{ t('user.upload_file') }}</el-button
              >
            </el-upload>
            <span v-if="(!form.filename && !props.concatenateMode) || (props.concatenateMode && multipleFiles.length === 0)" class="not_exceed">{{ $t('common.not_exceed_50mb') }}</span>
          </el-form-item>
        </div>
        <el-form-item :label="t('ds.form.name')" prop="name">
          <el-input
            v-model="form.name"
            clearable
            :placeholder="$t('datasource.please_enter') + $t('common.empty') + t('ds.form.name')"
          />
        </el-form-item>
        <el-form-item :label="t('ds.form.description')">
          <el-input
            v-model="form.description"
            :placeholder="
              $t('datasource.please_enter') + $t('common.empty') + t('ds.form.description')
            "
            :rows="2"
            show-word-limit
            maxlength="200"
            clearable
            type="textarea"
          />
        </el-form-item>
        <div v-if="form.type !== 'excel'" style="margin-top: 16px">
          <el-form-item
            :label="form.type !== 'es' ? t('ds.form.host') : t('ds.form.address')"
            prop="host"
          >
            <el-input
              v-model="form.host"
              clearable
              :placeholder="
                $t('datasource.please_enter') +
                $t('common.empty') +
                (form.type !== 'es' ? t('ds.form.host') : t('ds.form.address'))
              "
            />
          </el-form-item>
          <el-form-item v-if="form.type !== 'es'" :label="t('ds.form.port')" prop="port">
            <el-input
              v-model="form.port"
              clearable
              :placeholder="$t('datasource.please_enter') + $t('common.empty') + t('ds.form.port')"
            />
          </el-form-item>
          <el-form-item :label="t('ds.form.username')">
            <el-input
              v-model="form.username"
              clearable
              :placeholder="
                $t('datasource.please_enter') + $t('common.empty') + t('ds.form.username')
              "
            />
          </el-form-item>
          <el-form-item :label="t('ds.form.password')">
            <el-input
              v-model="form.password"
              clearable
              :placeholder="
                $t('datasource.please_enter') + $t('common.empty') + t('ds.form.password')
              "
              type="password"
              show-password
            />
          </el-form-item>
          <el-form-item
            v-if="form.type !== 'dm' && form.type !== 'es'"
            :label="t('ds.form.database')"
            prop="database"
          >
            <el-input
              v-model="form.database"
              clearable
              :placeholder="
                $t('datasource.please_enter') + $t('common.empty') + t('ds.form.database')
              "
            />
          </el-form-item>
          <el-form-item
            v-if="form.type === 'oracle'"
            :label="t('ds.form.connect_mode')"
            prop="mode"
          >
            <el-radio-group v-model="form.mode">
              <el-radio value="service_name">{{ t('ds.form.mode.service_name') }}</el-radio>
              <el-radio value="sid">{{ t('ds.form.mode.sid') }}</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="form.type !== 'es'" :label="t('ds.form.extra_jdbc')">
            <el-input
              v-model="form.extraJdbc"
              clearable
              :placeholder="
                $t('datasource.please_enter') + $t('common.empty') + t('ds.form.extra_jdbc')
              "
            />
          </el-form-item>
          <el-form-item v-if="haveSchema.includes(form.type)" class="schema-label" prop="dbSchema">
            <template #label>
              <span class="name">Schema<i class="required" /></span>
              <el-button text size="small" @click="getSchema">
                <template #icon>
                  <Icon name="icon_add_outlined">
                    <Plus class="svg-icon" />
                  </Icon>
                </template>
                {{ t('datasource.get_schema') }}
              </el-button>
            </template>
            <el-select
              v-model="form.dbSchema"
              filterable
              :placeholder="$t('datasource.please_enter') + $t('common.empty') + 'Schema'"
            >
              <el-option
                v-for="item in schemaList"
                :key="item.value"
                :label="item.label"
                :value="item.value"
              />
            </el-select>
          </el-form-item>
          <el-form-item v-if="form.type !== 'es'" :label="t('ds.form.timeout')" prop="timeout">
            <el-input-number
              v-model="form.timeout"
              clearable
              :min="0"
              :max="300"
              controls-position="right"
            />
          </el-form-item>
        </div>
      </el-form>
      <div v-show="activeStep === 2" v-loading="tableListLoading" class="select-data_table">
        <div class="title">
          {{ $t('ds.form.choose_tables') }} ({{ checkTableList.length }}/ {{ tableList.length }})
        </div>
        <el-input
          v-model="keywords"
          clearable
          style="width: 100%; margin-bottom: 16px"
          :placeholder="$t('datasource.search')"
        >
          <template #prefix>
            <el-icon>
              <icon_searchOutline_outlined class="svg-icon" />
            </el-icon>
          </template>
        </el-input>
        <div class="container">
          <div class="select-all">
            <el-checkbox
              v-model="checkAll"
              :indeterminate="isIndeterminate"
              @change="handleCheckAllChange"
            >
              {{ t('datasource.select_all') }}
            </el-checkbox>
          </div>
          <!-- 纵向合并模式：显示合并确认信息 -->
          <div v-if="props.concatenateMode" class="concatenate-confirm">
            <div class="title">确认合并信息</div>
            <div class="file-list">
              <div class="subtitle">待合并文件 ({{ multipleFiles.length }} 个):</div>
              <div v-for="(file, index) in multipleFiles" :key="index" class="file-item">
                <el-icon><icon_fileExcel_colorful /></el-icon>
                <span>{{ file.name }}</span>
                <span class="file-size">({{ formatFileSize(file.size) }})</span>
              </div>
            </div>
            <el-form ref="dsFormRef" :model="form" label-position="top">
              <el-form-item label="数据源名称" prop="name">
                <el-input v-model="form.name" placeholder="请输入合并后的数据源名称" />
              </el-form-item>
              <el-form-item label="描述">
                <el-input
                  v-model="form.description"
                  type="textarea"
                  :rows="3"
                  placeholder="请输入描述信息"
                  maxlength="200"
                  show-word-limit
                />
              </el-form-item>
            </el-form>
          </div>
          <!-- 普通模式：显示表选择 -->
          <div v-else>
            <EmptyBackground
              v-if="!!keywords && !tableListWithSearch.length"
              :description="$t('datasource.relevant_content_found')"
              img-type="tree"
              style="width: 100%"
            />
            <el-checkbox-group
              v-else
              v-model="checkList"
              style="position: relative"
              @change="handleCheckedTablesChange"
            >
              <FixedSizeList
                :item-size="32"
                :data="tableListWithSearch"
                :total="tableListWithSearch.length"
                :width="800"
                :height="460"
                :scrollbar-always-on="true"
                class-name="ed-select-dropdown__list"
                layout="vertical"
              >
                <template #default="{ index, style }">
                  <div class="list-item_primary" :style="style">
                    <el-checkbox :label="tableListWithSearch[index].tableName">
                      <el-icon size="16" style="margin-right: 8px">
                        <icon_form_outlined></icon_form_outlined>
                      </el-icon>
                      {{ tableListWithSearch[index].tableName }}</el-checkbox
                    >
                  </div>
                </template>
              </FixedSizeList>
            </el-checkbox-group>
          </div>
        </div>
      </div>
    </div>
    <div class="draw-foot">
      <el-button secondary @click="close">{{ t('common.cancel') }}</el-button>
      <el-button v-show="form.type !== 'excel' && !isDataTable" secondary @click="check">
        {{ t('ds.check') }}
      </el-button>
      <el-button v-show="activeStep !== 0 && isCreate" secondary @click="preview">
        {{ t('ds.previous') }}
      </el-button>
      <el-button v-show="activeStep === 1 && isCreate" type="primary" @click="next(dsFormRef)">
        {{ t('common.next') }}
      </el-button>
      <el-button v-show="activeStep === 2 || !isCreate" type="primary" @click="save(dsFormRef)">
        {{ props.concatenateMode ? '合并文件' : t('common.save') }}
      </el-button>
    </div>
  </div>
</template>

<style lang="less" scoped>
.model-form {
  width: calc(100% - 280px);
  position: absolute;
  right: 0;
  top: 56px;
  height: 100%;
  padding-bottom: 120px;
  overflow-y: auto;
  .model-name {
    height: 56px;
    width: 100%;
    padding-left: 24px;
    border-bottom: 1px solid #1f232926;
    font-weight: 500;
    font-size: 16px;
    line-height: 24px;
    display: flex;
    align-items: center;
  }

  .form-content {
    width: 800px;
    margin: 0 auto;
    padding-top: 24px;

    .upload-user {
      height: 32px;
      .ed-upload {
        width: 100% !important;
      }
    }

    .not_exceed {
      font-weight: 400;
      font-size: 14px;
      line-height: 22px;
      color: #8f959e;
      display: inline-block;
      width: 100%;
    }

    .pdf-card {
      width: 100%;
      height: 58px;
      display: flex;
      align-items: center;
      padding: 0 16px 0 12px;
      border: 1px solid #dee0e3;
      border-radius: 6px;

      .file-name {
        margin-left: 8px;
        .name {
          font-weight: 400;
          font-size: 14px;
          line-height: 22px;
        }

        .size {
          font-weight: 400;
          font-size: 12px;
          line-height: 20px;
          color: #8f959e;
        }
      }

      .action-btn {
        margin-left: auto;
      }

      .ed-icon {
        position: relative;
        cursor: pointer;
        color: #646a73;

        &::after {
          content: '';
          background-color: #1f23291a;
          position: absolute;
          border-radius: 6px;
          width: 24px;
          height: 24px;
          transform: translate(-50%, -50%);
          top: 50%;
          left: 50%;
          display: none;
        }

        &:hover {
          &::after {
            display: block;
          }
        }
      }
    }

    .ed-form-item--default {
      margin-bottom: 16px;

      &.is-error {
        margin-bottom: 40px;
      }
    }
  }

  :deep(.draw-foot) {
    position: fixed;
    bottom: 0;
    right: 0;
    width: calc(100% - 280px);
    height: 64px;
    display: flex;
    align-items: center;
    justify-content: flex-end;
    border-top: 1px solid #1f232926;
    padding-right: 24px;
    background-color: #fff;
    z-index: 10;
  }

  &.edit-form {
    width: 100%;

    :deep(.draw-foot) {
      width: 100%;
    }
  }
  .select-data_table {
    padding-bottom: 24px;
    .title {
      font-weight: 500;
      font-size: 16px;
      line-height: 24px;
      margin: 0 0 16px 0;
    }
    .container {
      border: 1px solid #dee0e3;
      border-radius: 4px;
      overflow: hidden;

      .select-all {
        background: #f5f6f7;
        height: 40px;
        padding-left: 12px;
        display: flex;
        align-items: center;
        border-bottom: 1px solid #dee0e3;
      }

      :deep(.ed-checkbox__label) {
        display: inline-flex;
        align-items: center;
      }

      :deep(.ed-vl__window) {
        scrollbar-width: none;
      }
    }
  }
}

.schema-label {
  ::v-deep(.ed-form-item__label) {
    display: flex !important;
    justify-content: space-between;
    padding-right: 0;

    &::after {
      display: none;
    }

    .name {
      .required::after {
        content: '*';
        color: #f54a45;
        margin-left: 2px;
      }
    }
  }
}

.concatenate-confirm {
  padding: 20px;
  
  .title {
    font-weight: 500;
    font-size: 16px;
    line-height: 24px;
    margin-bottom: 20px;
  }
  
  .file-list {
    margin-bottom: 24px;
    
    .subtitle {
      font-weight: 500;
      margin-bottom: 12px;
      color: #606266;
    }
    
    .file-item {
      display: flex;
      align-items: center;
      padding: 8px 12px;
      background: #f5f7fa;
      border-radius: 4px;
      margin-bottom: 8px;
      
      .el-icon {
        margin-right: 8px;
        font-size: 16px;
      }
      
      span {
        margin-right: 8px;
      }
      
      .file-size {
        color: #909399;
        font-size: 12px;
      }
    }
  }
}
</style>
