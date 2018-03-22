# -*- coding: utf-8 -*-

from datetime import datetime
from elasticsearch_dsl import DocType, Date, Nested, Boolean, \
    analyzer, InnerObjectWrapper, Completion, Keyword, Text, Integer

from elasticsearch_dsl.analysis import CustomAnalyzer as _CustomAnalyzer

from elasticsearch_dsl.connections import connections
connections.create_connection(hosts=["localhost"])

class CustomAnalyzer(_CustomAnalyzer):
    def get_analysis_definition(self):
        return {}


ik_analyzer = CustomAnalyzer("ik_max_word", filter=["lowercase"])

class JdSpiderType(DocType):
    suggest = Completion(analyzer=ik_analyzer)
    item_id = Keyword()
    name = Text(analyzer="ik_max_word")
    summary = Text(analyzer="ik_max_word")
    price = Integer()
    tag_1 = Text(analyzer="ik_max_word")
    tag_2 = Text(analyzer="ik_max_word")
    tag_3 = Text(analyzer="ik_max_word")
    tag_4 = Text(analyzer="ik_max_word")
    dianpu_name = Text(analyzer="ik_max_word")
    jself = Integer()
    crawl_time = Date()

    class Meta:
        index = "spider"
        doc_type = "jd"


if __name__ == "__main__":
    JdSpiderType.init()