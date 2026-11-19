"""校园成果库的 OAI-PMH 合成服务。"""
import json
from urllib.parse import urlencode
from xml.sax.saxutils import escape

NS="http://www.openarchives.org/OAI/2.0/"
def xml(content):
    return 200,{"Content-Type":"text/xml; charset=utf-8"},f'<OAI-PMH xmlns="{NS}"><responseDate>2026-11-16T10:00:05Z</responseDate><request>/oai</request>{content}</OAI-PMH>'
def error(code,text):
    return xml(f'<error code="{code}">{escape(text)}</error>')

class Source:
    name="校园成果库"
    def __init__(self):
        self.tokens={}
        self.serial=0
    def records(self):
        rows=[
            dict(id="oai:campus:p1",stamp="2026-11-16T10:00:00Z",sets=["teaching","industry"],title="场景化软件实践",deleted=False),
            dict(id="oai:campus:p2",stamp="2026-11-16T10:00:00Z",sets=["teaching"],title="基础编程实验",deleted=False),
            dict(id="oai:campus:p3",stamp="2026-11-16T10:00:01Z",sets=["industry"],title="工程协作训练",deleted=False),
            dict(id="oai:campus:p4",stamp="2026-11-16T10:00:02Z",sets=["teaching"],title="判断力课程",deleted=False),
        ]
        if self.stage>=1:
            rows[0]["title"]="场景化软件实践（补充版）"
        if self.stage>=2:
            rows[0].update(sets=["industry"],stamp="2026-11-16T10:00:03Z")
        if self.stage>=3:
            rows[0].update(deleted=True,stamp="2026-11-16T10:00:04Z")
        return rows
    def item(self,row,fmt,header_only=False):
        sets="".join(f"<setSpec>{s}</setSpec>" for s in row["sets"])
        h=f'<header{chr(32)+"status="+chr(34)+"deleted"+chr(34) if row["deleted"] else ""}><identifier>{row["id"]}</identifier><datestamp>{row["stamp"]}</datestamp>{sets}</header>'
        if header_only:return h
        if row["deleted"]:metadata=""
        elif fmt=="oai_dc":
            metadata=f'<metadata><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>{row["title"]}</dc:title><dc:identifier>{row["id"]}</dc:identifier></oai_dc:dc></metadata>'
        else:metadata=f'<metadata><description xmlns="urn:campus:research"><title>{row["title"]}</title><level>完整研究</level></description></metadata>' 
        return "<record>"+h+metadata+"</record>"
    def route(self,method,path,q,headers,body):
        if path=="/research.xsd":
            return 200,{"Content-Type":"application/xml"},'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" targetNamespace="urn:campus:research" elementFormDefault="qualified"><xs:element name="description"><xs:complexType><xs:sequence><xs:element name="title" type="xs:string"/><xs:element name="level" type="xs:string"/></xs:sequence></xs:complexType></xs:element></xs:schema>'
        if path!="/oai":return 404,{},b""
        verb=q.get("verb","")
        if verb=="Identify":
            return xml("<Identify><repositoryName>校园成果库</repositoryName><baseURL>BASE_URL</baseURL><protocolVersion>2.0</protocolVersion><adminEmail>library@example.invalid</adminEmail><earliestDatestamp>2026-11-16T10:00:00Z</earliestDatestamp><deletedRecord>persistent</deletedRecord><granularity>YYYY-MM-DDThh:mm:ssZ</granularity></Identify>".replace("BASE_URL",self.base+"/oai"))
        if verb=="ListSets":
            return xml("<ListSets><set><setSpec>teaching</setSpec><setName>教学创新</setName></set><set><setSpec>industry</setSpec><setName>产业实践</setName></set></ListSets>")
        if verb=="ListMetadataFormats":
            formats=[("oai_dc","http://www.openarchives.org/OAI/2.0/oai_dc.xsd","http://www.openarchives.org/OAI/2.0/oai_dc/"),("research",self.base+"/research.xsd","urn:campus:research")]
            return xml("<ListMetadataFormats>"+"".join(f"<metadataFormat><metadataPrefix>{f}</metadataPrefix><schema>{schema}</schema><metadataNamespace>{ns}</metadataNamespace></metadataFormat>" for f,schema,ns in formats)+"</ListMetadataFormats>")
        if verb not in ["ListRecords","ListIdentifiers","GetRecord"]:return error("badVerb","不支持的动词")
        if "resumptionToken" in q:
            if set(q)!={"verb","resumptionToken"}:return error("badArgument","续页不能混入原查询")
            token=self.tokens.get(q["resumptionToken"])
            if not token or token["stage"]!=self.stage or self.options.get("expire"):
                return error("badResumptionToken","凭证已过期")
            params,offset=token["params"],token["offset"]
        else:params,offset=q,0
        fmt=params.get("metadataPrefix","")
        if fmt not in ["oai_dc","research"]:return error("cannotDisseminateFormat","未支持格式")
        if verb=="GetRecord":
            rows=[r for r in self.records() if r["id"]==params.get("identifier")]
            return xml("<GetRecord>"+self.item(rows[0],fmt)+"</GetRecord>") if rows else error("idDoesNotExist","标识不存在")
        rows=[r for r in self.records() if (not params.get("set") or params["set"] in r["sets"]) and params.get("from","")<=r["stamp"]<=params.get("until","9999")]
        if not rows:return error("noRecordsMatch","范围内无记录")
        page=rows[offset:offset+2]
        cursor=""
        if offset+2<len(rows):
            self.serial+=1
            t="opaque-"+str(self.serial)
            self.tokens[t]=dict(params=params,offset=offset+2,stage=self.stage)
            cursor=f'<resumptionToken cursor="{offset}" completeListSize="{len(rows)}">{t}</resumptionToken>'
        else:cursor=f'<resumptionToken cursor="{offset}" completeListSize="{len(rows)}" />'
        return xml("<"+verb+">"+"".join(self.item(r,fmt,verb=="ListIdentifiers") for r in page)+cursor+"</"+verb+">")
