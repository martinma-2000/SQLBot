import { request } from '@/utils/request'
import { getDate } from '@/utils/utils.ts'

export const questionApi = {
  pager: (pageNumber: number, pageSize: number) =>
    request.get(`/chat/question/pager/${pageNumber}/${pageSize}`),
  /* add: (data: any) => new Promise((resolve, reject) => {
      request.post('/chat/question', data, { responseType: 'stream', timeout: 0, onDownloadProgress: p => {
        resolve(p)
      }}).catch(e => reject(e))
    }), */
  // add: (data: any) => request.post('/chat/question', data),
  add: (data: any, controller?: AbortController) =>
    request.fetchStream('/chat/question', data, controller),
  edit: (data: any) => request.put('/chat/question', data),
  delete: (id: number) => request.delete(`/chat/question/${id}`),
  query: (id: number) => request.get(`/chat/question/${id}`),
}

export interface ChatMessage {
  role: 'user' | 'assistant'
  create_time?: Date | string
  content?: string | number
  record?: ChatRecord
  isTyping?: boolean
  first_chat?: boolean
  recommended_question?: string
  index: number
}

export class ChatRecord {
  id?: number
  chat_id?: number
  create_time?: Date | string
  finish_time?: Date | string
  question?: string
  sql_answer?: string
  sql?: string
  data?: string | any
  chart_answer?: string
  chart?: string
  analysis?: string
  analysis_thinking?: string
  predict?: string
  predict_content?: string
  predict_data?: string | any
  finish?: boolean = false
  error?: string
  run_time: number = 0
  first_chat: boolean = false
  recommended_question?: string
  analysis_record_id?: number
  predict_record_id?: number

  constructor()
  constructor(
    id: number,
    chat_id: number,
    create_time: Date | string,
    finish_time: Date | string | undefined,
    question: string,
    sql_answer: string | undefined,
    sql: string | undefined,
    data: string | any | undefined,
    chart_answer: string | undefined,
    chart: string | undefined,
    analysis: string | undefined,
    analysis_thinking: string | undefined,
    predict: string | undefined,
    predict_content: string | undefined,
    predict_data: string | any | undefined,
    finish: boolean,
    error: string | undefined,
    run_time: number,
    first_chat: boolean,
    recommended_question: string | undefined,
    analysis_record_id: number | undefined,
    predict_record_id: number | undefined
  )
  constructor(
    id?: number,
    chat_id?: number,
    create_time?: Date | string,
    finish_time?: Date | string,
    question?: string,
    sql_answer?: string,
    sql?: string,
    data?: string | any,
    chart_answer?: string,
    chart?: string,
    analysis?: string,
    analysis_thinking?: string,
    predict?: string,
    predict_content?: string,
    predict_data?: string | any,
    finish?: boolean,
    error?: string,
    run_time?: number,
    first_chat?: boolean,
    recommended_question?: string,
    analysis_record_id?: number,
    predict_record_id?: number
  ) {
    this.id = id
    this.chat_id = chat_id
    this.create_time = getDate(create_time)
    this.finish_time = getDate(finish_time)
    this.question = question
    this.sql_answer = sql_answer
    this.sql = sql
    this.data = data
    this.chart_answer = chart_answer
    this.chart = chart
    this.analysis = analysis
    this.analysis_thinking = analysis_thinking
    this.predict = predict
    this.predict_content = predict_content
    this.predict_data = predict_data
    this.finish = !!finish
    this.error = error
    this.run_time = run_time ?? 0
    this.first_chat = !!first_chat
    this.recommended_question = recommended_question
    this.analysis_record_id = analysis_record_id
    this.predict_record_id = predict_record_id
  }
}

export class Chat {
  id?: number
  create_time?: Date | string
  create_by?: number
  brief?: string
  chat_type?: string
  datasource?: number
  engine_type?: string
  ds_type?: string

  constructor()
  constructor(
    id: number,
    create_time: Date | string,
    create_by: number,
    brief: string,
    chat_type: string,
    datasource: number,
    engine_type: string
  )
  constructor(
    id?: number,
    create_time?: Date | string,
    create_by?: number,
    brief?: string,
    chat_type?: string,
    datasource?: number,
    engine_type?: string
  ) {
    this.id = id
    this.create_time = getDate(create_time)
    this.create_by = create_by
    this.brief = brief
    this.chat_type = chat_type
    this.datasource = datasource
    this.engine_type = engine_type
  }
}

export class ChatInfo extends Chat {
  datasource_name?: string
  datasource_exists: boolean = true
  records: Array<ChatRecord> = []

  constructor()
  constructor(chat: Chat)
  constructor(
    id: number,
    create_time: Date | string,
    create_by: number,
    brief: string,
    chat_type: string,
    datasource: number,
    engine_type: string,
    ds_type: string,
    datasource_name: string,
    datasource_exists: boolean,
    records: Array<ChatRecord>
  )
  constructor(
    param1?: number | Chat,
    create_time?: Date | string,
    create_by?: number,
    brief?: string,
    chat_type?: string,
    datasource?: number,
    engine_type?: string,
    ds_type?: string,
    datasource_name?: string,
    datasource_exists: boolean = true,
    records: Array<ChatRecord> = []
  ) {
    super()
    if (param1 !== undefined) {
      if (param1 instanceof Chat) {
        this.id = param1.id
        this.create_time = getDate(param1.create_time)
        this.create_by = param1.create_by
        this.brief = param1.brief
        this.chat_type = param1.chat_type
        this.datasource = param1.datasource
        this.engine_type = param1.engine_type
        this.ds_type = param1.ds_type
      } else {
        this.id = param1
        this.create_time = getDate(create_time)
        this.create_by = create_by
        this.brief = brief
        this.chat_type = chat_type
        this.datasource = datasource
        this.engine_type = engine_type
        this.ds_type = ds_type
      }
    }
    this.datasource_name = datasource_name
    this.datasource_exists = datasource_exists
    this.records = records
  }
}

const toChatRecord = (data?: any): ChatRecord | undefined => {
  if (!data) {
    return undefined
  }
  return new ChatRecord(
    data.id,
    data.chat_id,
    data.create_time,
    data.finish_time,
    data.question,
    data.sql_answer,
    data.sql,
    data.data,
    data.chart_answer,
    data.chart,
    data.analysis,
    data.analysis_thinking,
    data.predict,
    data.predict_content,
    data.predict_data,
    data.finish,
    data.error,
    data.run_time,
    data.first_chat,
    data.recommended_question,
    data.analysis_record_id,
    data.predict_record_id
  )
}
const toChatRecordList = (list: any = []): ChatRecord[] => {
  const records: Array<ChatRecord> = []
  for (let i = 0; i < list.length; i++) {
    const record = toChatRecord(list[i])
    if (record) {
      records.push(record)
    }
  }
  return records
}

export const chatApi = {
  toChatInfo: (data?: any): ChatInfo | undefined => {
    if (!data) {
      return undefined
    }
    return new ChatInfo(
      data.id,
      data.create_time,
      data.create_by,
      data.brief,
      data.chat_type,
      data.datasource,
      data.engine_type,
      data.ds_type,
      data.datasource_name,
      data.datasource_exists,
      toChatRecordList(data.records)
    )
  },
  toChatInfoList: (list: any[] = []): ChatInfo[] => {
    const infos: Array<ChatInfo> = []
    for (let i = 0; i < list.length; i++) {
      const chatInfo = chatApi.toChatInfo(list[i])
      if (chatInfo) {
        infos.push(chatInfo)
      }
    }
    return infos
  },
  list: (): Promise<Array<ChatInfo>> => {
    return request.get('/chat/list')
  },
  get: (id: number): Promise<ChatInfo> => {
    return request.get(`/chat/get/${id}`)
  },
  get_with_Data: (id: number): Promise<ChatInfo> => {
    return request.get(`/chat/get/with_data/${id}`)
  },
  get_chart_data: (record_id?: number): Promise<any> => {
    return request.get(`/chat/record/get/${record_id}/data`)
  },
  get_chart_predict_data: (record_id?: number): Promise<any> => {
    return request.get(`/chat/record/get/${record_id}/predict_data`)
  },
  startChat: (data: any): Promise<ChatInfo> => {
    return request.post('/chat/start', data)
  },
  startAssistantChat: (): Promise<ChatInfo> => {
    return request.post('/chat/assistant/start')
  },
  renameChat: (chat_id: number | undefined, brief: string): Promise<string> => {
    return request.post('/chat/rename', { id: chat_id, brief: brief })
  },
  deleteChat: (id: number | undefined): Promise<string> => {
    return request.get(`/chat/delete/${id}`)
  },
  analysis: (record_id: number | undefined, controller?: AbortController) => {
    return request.fetchStream(`/chat/record/${record_id}/analysis`, {}, controller)
  },
  predict: (record_id: number | undefined, controller?: AbortController) => {
    return request.fetchStream(`/chat/record/${record_id}/predict`, {}, controller)
  },
  recommendQuestions: (record_id: number | undefined, controller?: AbortController) => {
    return request.fetchStream(`/chat/recommend_questions/${record_id}`, {}, controller)
  },
  checkLLMModel: () => request.get('/system/aimodel/default', { requestOptions: { silent: true } }),
  export2Excel: (data: any) =>
    request.post('/chat/excel/export', data, {
      responseType: 'blob',
      requestOptions: { customError: true },
    }),
}
