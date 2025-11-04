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
  }>(),
  {
    activeName: '',
    activeType: '',
    activeStep: 0,
    isDataTable: false,
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
  sheets: [{ required: true, message: t('user.upload_file'), trigger: 'change' }],
  dbSchema: [
    {
      required: true,
      message: t('datasource.please_enter') + t('common.empty') + 'Schema',
      trigger: 'blur',
    },
  ],
  // API数据源验证规则
  endpoint: [
    {
      required: true,
      message: '请输入API端点URL',
      trigger: 'blur',
    },
    {
      type: 'url',
      message: '请输入有效的URL地址',
      trigger: 'blur',
    },
  ],
  timeout: [
    {
      required: true,
      message: '请输入超时时间',
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
  // 来源标记：local 或 api
  originType: 'local',
  // API数据源字段
  endpoint: '',
  method: 'POST',
  date_m: '',
  p_date_m: '',
  headerKey: '',
  headerValue: '',
  cookieKey: '',
  cookieValue: '',
  paramKey: '',
  paramValue: '',
  certificate: '',
  certificateList: [],
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
      
      if (item.type === 'api') {
        // API 类型默认来源为 api
        form.value.originType = 'api'
        // 加载API数据源配置
        form.value.endpoint = configuration.endpoint
        form.value.method = configuration.method || 'POST'
        form.value.requestBody = configuration.requestBody || ''
        form.value.timeout = configuration.timeout ? configuration.timeout : 30
        
        // 加载成功状态码
        if (configuration.successStatusCodes && Array.isArray(configuration.successStatusCodes)) {
          form.value.successStatusCodes = configuration.successStatusCodes.join(',')
        } else {
          form.value.successStatusCodes = '200,201'
        }
        
        // 加载认证信息
        form.value.certificateList = configuration.certificateList || []
        
        if (item.certificate) {
          try {
            const certificateList = JSON.parse(item.certificate)
            for (const cert of certificateList) {
              if (cert.target === 'header') {
                form.value.headerKey = cert.key
                form.value.headerValue = cert.value
              } else if (cert.target === 'cookie') {
                form.value.cookieKey = cert.key
                form.value.cookieValue = cert.value
              } else if (cert.target === 'param') {
                form.value.paramKey = cert.key
                form.value.paramValue = cert.value
              }
            }
          } catch (e) {
            console.error('解析certificate失败', e)
          }
        }
      } else {
        // 加载传统数据库配置
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
        // 兼容历史数据：没有标记则默认为本地
        form.value.originType = configuration.originType || 'local'
      }
    }

    if (editTable) {
      dialogTitle.value = t('ds.form.choose_tables')
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
      originType: 'local',
    }
  }
  dialogVisible.value = true
}

const save = async (formEl: FormInstance | undefined) => {
  if (!formEl) return
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
  if (form.value.type === 'api') {
    // 处理API数据源配置
    const certificateList = [];
    
    // 添加header认证
    if (form.value.headerKey && form.value.headerValue) {
      certificateList.push({
        target: 'header',
        key: form.value.headerKey,
        value: form.value.headerValue
      });
    }
    
    // 添加cookie认证
    if (form.value.cookieKey && form.value.cookieValue) {
      certificateList.push({
        target: 'cookie',
        key: form.value.cookieKey,
        value: form.value.cookieValue
      });
    }
    
    // 添加URL参数认证
    if (form.value.paramKey && form.value.paramValue) {
      certificateList.push({
        target: 'param',
        key: form.value.paramKey,
        value: form.value.paramValue
      });
    }
    
    // 处理成功状态码
    let successStatusCodes = [200, 201];
    if (form.value.successStatusCodes) {
      successStatusCodes = form.value.successStatusCodes.split(',').map((code: string) => parseInt(code.trim()));
    }
    
    form.value.configuration = encrypted(
      JSON.stringify({
        endpoint: form.value.endpoint,
        method: form.value.method || 'POST',
        requestBody: form.value.requestBody || '',
        successStatusCodes: successStatusCodes,
        certificateList: certificateList,
        timeout: form.value.timeout || 30,
        // 标记来源为 API
        originType: 'api'
      })
    );
    
    form.value.certificate = JSON.stringify(certificateList);
    
    const obj = JSON.parse(JSON.stringify(form.value));
    delete obj.endpoint;
    delete obj.headerKey;
    delete obj.headerValue;
    delete obj.cookieKey;
    delete obj.cookieValue;
    delete obj.paramKey;
    delete obj.paramValue;
    delete obj.timeout;
    delete obj.originType;
    
    // 删除传统数据库字段
    delete obj.driver;
    delete obj.host;
    delete obj.port;
    delete obj.username;
    delete obj.password;
    delete obj.database;
    delete obj.extraJdbc;
    delete obj.dbSchema;
    delete obj.filename;
    delete obj.sheets;
    delete obj.mode;
    
    return obj;
  } else {
    // 处理传统数据库配置
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
        // 标记来源：默认 local；若由 API 转换则为 api
        originType: form.value.originType || 'local',
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
    delete obj.originType
    return obj
  }
}

const check = () => {
  // 在 API 类型下，执行“API连通性/下载测试”；其他类型保持原校验逻辑
  if (form.value.type === 'api') {
    const apiReq = {
      endpoint: form.value.endpoint,
      method: form.value.method || 'GET',
      date_m: form.value.date_m,
      p_date_m: form.value.p_date_m,
      headerKey: form.value.headerKey,
      headerValue: form.value.headerValue,
      cookieKey: form.value.cookieKey,
      cookieValue: form.value.cookieValue,
      paramKey: form.value.paramKey,
      paramValue: form.value.paramValue,
      timeout: form.value.timeout || 30,
      separator: '_',
    }
    datasourceApi
      .testApiExcel(apiReq)
      .then((res: any) => {
        const sheetCount = (res?.sheet_names ?? []).length
        ElMessage({
          message: `API联通正常，检测到${sheetCount}个Sheet`,
          type: 'success',
          showClose: true,
        })
      })
      .catch((err: any) => {
        console.error('Test API Excel failed:', err)
        ElMessage({
          message: 'API联通或文件类型校验失败',
          type: 'error',
          showClose: true,
        })
      })
    return
  }

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
  await formEl.validate((valid) => {
    if (valid) {
      if (form.value.type === 'excel') {
        // next, show tables
        if (excelUploadSuccess.value) {
          emit('changeActiveStep', props.activeStep + 1)
        }
      } else if (form.value.type === 'api') {
        // API数据源：通过填写url和参数，服务端拉取并处理Excel，然后进入表选择步骤
        if (uploadLoading.value) return
        uploadLoading.value = true
        const apiReq = {
          endpoint: form.value.endpoint,
          method: form.value.method || 'GET',
          date_m: form.value.date_m,
          p_date_m: form.value.p_date_m,
          headerKey: form.value.headerKey,
          headerValue: form.value.headerValue,
          cookieKey: form.value.cookieKey,
          cookieValue: form.value.cookieValue,
          paramKey: form.value.paramKey,
          paramValue: form.value.paramValue,
          timeout: form.value.timeout || 30,
          separator: '_',
        }
        datasourceApi
          .fetchExcelFromApi(apiReq)
          .then((res: any) => {
            // 与本地Excel上传一致的返回结构
            const data = res
            form.value.filename = data.filename
            form.value.sheets = data.sheets
            tableList.value = data.sheets
            excelUploadSuccess.value = true
            // 切换到excel流程以保持后续一致
            form.value.type = 'excel'
            // 标记来源为 API，供后续展示时使用
            form.value.originType = 'api'
            emit('changeActiveStep', props.activeStep + 1)
          })
          .catch((err: any) => {
            console.error('Fetch Excel from API failed:', err)
            ElMessage({
              message: '通过API获取Excel失败',
              type: 'error',
              showClose: true,
            })
          })
          .finally(() => {
            uploadLoading.value = false
          })
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
  if (typeof setFile === 'function') {
    setFile(rawFile)
  }
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

const onError = (error: any) => {
  uploadLoading.value = false
  console.error('Upload error:', error)
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
  }
)
const fileSize = ref('-')
const clearFile = () => {
  fileSize.value = ''
  form.value.filename = ''
  form.value.sheets = []
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
        <div v-if="form.type === 'excel'">
          <el-form-item prop="sheets" :label="t('ds.form.file')">
            <div v-if="form.filename" class="pdf-card">
              <img :src="icon_fileExcel_colorful" width="40px" height="40px" />
              <div class="file-name">
                <div class="name">{{ form.filename }}</div>
                <div class="size">{{ form.filename.split('.')[1] }} - {{ fileSize }}</div>
              </div>
              <el-icon v-if="!form.id" class="action-btn" size="16" @click="clearFile">
                <IconOpeDelete></IconOpeDelete>
              </el-icon>
            </div>
            <el-upload
              v-if="form.filename && !form.id"
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
              v-else-if="!form.id"
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
            <span v-if="!form.filename" class="not_exceed">{{ $t('common.not_exceed_50mb') }}</span>
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
        <div v-if="form.type === 'api'" style="margin-top: 16px">
          <!-- API数据源表单 -->
          <el-form-item label="API端点" prop="endpoint">
            <el-input
              v-model="form.endpoint"
              clearable
              placeholder="请输入API端点URL"
            />
          </el-form-item>
          
          <!-- HTTP方法选择 -->
          <el-form-item label="HTTP方法" prop="method">
            <el-select v-model="form.method" placeholder="请选择HTTP方法">
              <el-option label="GET" value="GET" />
              <el-option label="POST" value="POST" />
              <el-option label="PUT" value="PUT" />
              <el-option label="DELETE" value="DELETE" />
            </el-select>
          </el-form-item>
          
          <!-- date_m参数 -->
          <el-form-item label="date_m" prop="date_m">
            <el-input
              v-model="form.date_m"
              clearable
              placeholder="例如: 2025-03-31"
            />
          </el-form-item>
          
          <!-- p_date_m参数 -->
          <el-form-item label="p_date_m" prop="p_date_m">
            <el-input
              v-model="form.p_date_m"
              clearable
              placeholder="例如: 2025-03"
            />
          </el-form-item>
          
          <!-- 超时设置 -->
          <el-form-item label="超时时间(秒)" prop="timeout">
            <el-input-number
              v-model="form.timeout"
              :min="1"
              :max="300"
              :step="1"
              style="width: 100%"
            />
          </el-form-item>
          
          <!-- 认证信息 -->
          <div style="margin-bottom: 16px;">
            <div style="font-weight: 500; margin-bottom: 8px;">认证信息</div>
            
            <!-- Header认证 -->
            <div style="margin-bottom: 16px; border: 1px solid #EBEEF5; border-radius: 4px; padding: 16px;">
              <div style="font-weight: 500; margin-bottom: 8px;">Header认证</div>
              <el-row :gutter="12">
                <el-col :span="12">
                  <el-form-item label="Key">
                    <el-input v-model="form.headerKey" placeholder="请输入Header Key" />
                  </el-form-item>
                </el-col>
                <el-col :span="12">
                  <el-form-item label="Value">
                    <el-input v-model="form.headerValue" placeholder="请输入Header Value" />
                  </el-form-item>
                </el-col>
              </el-row>
            </div>
            
            <!-- Cookie认证 -->
            <div style="margin-bottom: 16px; border: 1px solid #EBEEF5; border-radius: 4px; padding: 16px;">
              <div style="font-weight: 500; margin-bottom: 8px;">Cookie认证</div>
              <el-row :gutter="12">
                <el-col :span="12">
                  <el-form-item label="Key">
                    <el-input v-model="form.cookieKey" placeholder="请输入Cookie Key" />
                  </el-form-item>
                </el-col>
                <el-col :span="12">
                  <el-form-item label="Value">
                    <el-input v-model="form.cookieValue" placeholder="请输入Cookie Value" />
                  </el-form-item>
                </el-col>
              </el-row>
            </div>
            
            <!-- URL参数认证 -->
            <div style="border: 1px solid #EBEEF5; border-radius: 4px; padding: 16px;">
              <div style="font-weight: 500; margin-bottom: 8px;">URL参数认证</div>
              <el-row :gutter="12">
                <el-col :span="12">
                  <el-form-item label="Key">
                    <el-input v-model="form.paramKey" placeholder="请输入URL参数 Key" />
                  </el-form-item>
                </el-col>
                <el-col :span="12">
                  <el-form-item label="Value">
                    <el-input v-model="form.paramValue" placeholder="请输入URL参数 Value" />
                  </el-form-item>
                </el-col>
              </el-row>
            </div>
          </div>
        </div>
        
        <div v-if="form.type !== 'excel' && form.type !== 'api'" style="margin-top: 16px">
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
    <div class="draw-foot">
      <el-button secondary @click="close">{{ t('common.cancel') }}</el-button>
      <el-button v-show="form.type !== 'excel' && !isDataTable" secondary @click="check">
        {{ form.type === 'api' ? '校验API联通' : t('ds.check') }}
      </el-button>
      <el-button v-show="activeStep !== 0 && isCreate" secondary @click="preview">
        {{ t('ds.previous') }}
      </el-button>
      <el-button v-show="activeStep === 1 && isCreate" type="primary" @click="next(dsFormRef)">
        {{ t('common.next') }}
      </el-button>
      <el-button v-show="activeStep === 2 || !isCreate" type="primary" @click="save(dsFormRef)">
        {{ t('common.save') }}
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
</style>
