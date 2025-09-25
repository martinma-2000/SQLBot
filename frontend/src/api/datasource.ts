import { request } from '@/utils/request'

export const datasourceApi = {
  check: (data: any) => request.post('/datasource/check', data),
  check_by_id: (id: any) => request.get(`/datasource/check/${id}`),
  add: (data: any) => request.post('/datasource/add', data),
  list: () => request.get('/datasource/list'),
  update: (data: any) => request.post('/datasource/update', data),
  delete: (id: number) => request.post(`/datasource/delete/${id}`),
  getTables: (id: number) => request.post(`/datasource/getTables/${id}`),
  getTablesByConf: (data: any) => request.post('/datasource/getTablesByConf', data),
  getFields: (id: number, table_name: string) =>
    request.post(`/datasource/getFields/${id}/${table_name}`),
  execSql: (id: number | string, sql: string) =>
    request.post(`/datasource/execSql/${id}`, { sql: sql }),
  chooseTables: (id: number, data: any) => request.post(`/datasource/chooseTables/${id}`, data),
  tableList: (id: number) => request.post(`/datasource/tableList/${id}`),
  fieldList: (id: number) => request.post(`/datasource/fieldList/${id}`),
  edit: (data: any) => request.post('/datasource/editLocalComment', data),
  previewData: (id: number, data: any) => request.post(`/datasource/previewData/${id}`, data),
  saveTable: (data: any) => request.post('/datasource/editTable', data),
  saveField: (data: any) => request.post('/datasource/editField', data),
  getDs: (id: number) => request.post(`/datasource/get/${id}`),
  cancelRequests: () => request.cancelRequests(),
  getSchema: (data: any) => request.post('/datasource/getSchemaByConf', data),
  // Upload multiple excel/csv files to concatenate vertically on server, returns merged file blob
  concatenateExcels: (formData: FormData) =>
    request.post('/datasource/concatenateExcels', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    }),
}
