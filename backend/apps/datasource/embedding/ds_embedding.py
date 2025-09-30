# Author: Junjun
# Date: 2025/9/18
import json
import traceback
from typing import Optional

from apps.ai_model.embedding import EmbeddingModelCache
from apps.datasource.crud.datasource import get_table_schema
from apps.datasource.embedding.utils import cosine_similarity
from apps.datasource.models.datasource import CoreDatasource
from apps.system.crud.assistant import AssistantOutDs
from common.core.deps import CurrentAssistant
from common.core.deps import SessionDep, CurrentUser
from common.utils.utils import SQLBotLogUtil


def get_ds_embedding(session: SessionDep, current_user: CurrentUser, _ds_list, out_ds: AssistantOutDs,
                     question: str,
                     current_assistant: Optional[CurrentAssistant] = None):
    _list = []
    if current_assistant and current_assistant.type != 4:
        if out_ds.ds_list:
            for _ds in out_ds.ds_list:
                ds = out_ds.get_ds(_ds.id)
                table_schema = out_ds.get_db_schema(_ds.id)
                ds_info = f"{ds.name}, {ds.description}\n"
                ds_schema = ds_info + table_schema
                _list.append({"id": ds.id, "ds_schema": ds_schema, "cosine_similarity": 0.0, "ds": ds})
    else:
        for _ds in _ds_list:
            if _ds.get('id'):
                ds = session.get(CoreDatasource, _ds.get('id'))
                table_schema = get_table_schema(session, current_user, ds, question, embedding=False)
                ds_info = f"{ds.name}, {ds.description}\n"
                ds_schema = ds_info + table_schema
                _list.append({"id": ds.id, "ds_schema": ds_schema, "cosine_similarity": 0.0, "ds": ds})

    if _list:
        try:
            text = [s.get('ds_schema') for s in _list]

            model = EmbeddingModelCache.get_model()
            results = model.embed_documents(text)

            q_embedding = model.embed_query(question)
            for index in range(len(results)):
                item = results[index]
                _list[index]['cosine_similarity'] = cosine_similarity(q_embedding, item)

            _list.sort(key=lambda x: x['cosine_similarity'], reverse=True)
            # print(len(_list))
            SQLBotLogUtil.info(json.dumps(
                [{"id": ele.get("id"), "name": ele.get("ds").name, "cosine_similarity": ele.get("cosine_similarity")}
                 for ele in _list]))
            ds = _list[0].get('ds')
            return {"id": ds.id, "name": ds.name, "description": ds.description}
        except Exception:
            traceback.print_exc()
    return _list
