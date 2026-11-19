from tests.support import SourceCase
import xml.etree.ElementTree as E
NS={"o":"http://www.openarchives.org/OAI/2.0/"}
class Contract(SourceCase):
    def tree(self,q):
        status,_,data=self.request("/oai?"+q);self.assertEqual(status,200);return E.fromstring(data)
    def test_identify(self):
        self.assertEqual(self.tree("verb=Identify").findtext(".//o:deletedRecord",namespaces=NS),"persistent")
    def test_formats(self):
        self.assertEqual(len(self.tree("verb=ListMetadataFormats").findall(".//o:metadataFormat",NS)),2)
    def test_page_scope_and_end(self):
        t=self.tree("verb=ListRecords&metadataPrefix=research&set=teaching")
        token=t.findtext(".//o:resumptionToken",namespaces=NS)
        self.assertTrue(token)
        p=self.tree("verb=ListRecords&resumptionToken="+token)
        self.assertEqual(len(p.findall(".//o:record",NS)),1)
        self.assertEqual(p.find(".//o:resumptionToken",NS).text,None)
    def test_inclusive_same_second(self):
        t=self.tree("verb=ListRecords&metadataPrefix=oai_dc&from=2026-11-16T10:00:00Z&until=2026-11-16T10:00:00Z")
        self.assertEqual(len(t.findall(".//o:record",NS)),2)
    def test_membership_not_deletion(self):
        self.control(2)
        t=self.tree("verb=GetRecord&metadataPrefix=research&identifier=oai:campus:p1")
        self.assertEqual([x.text for x in t.findall(".//o:setSpec",NS)],["industry"])
        self.assertIsNone(t.find(".//o:header",NS).get("status"))
    def test_persistent_delete_both_formats(self):
        self.control(3)
        for fmt in ["research","oai_dc"]:
            t=self.tree("verb=GetRecord&metadataPrefix="+fmt+"&identifier=oai:campus:p1")
            self.assertEqual(t.find(".//o:header",NS).get("status"),"deleted")
            self.assertIsNone(t.find(".//o:metadata",NS))
    def test_expired_token_and_bad_combination(self):
        token=self.tree("verb=ListRecords&metadataPrefix=research").findtext(".//o:resumptionToken",namespaces=NS)
        self.assertEqual(self.tree("verb=ListRecords&resumptionToken="+token+"&set=teaching").find("o:error",NS).get("code"),"badArgument")
        self.control(1)
        self.assertEqual(self.tree("verb=ListRecords&resumptionToken="+token).find("o:error",NS).get("code"),"badResumptionToken")
    def test_empty_is_protocol_fact(self):
        self.assertEqual(self.tree("verb=ListRecords&metadataPrefix=research&set=absent").find("o:error",NS).get("code"),"noRecordsMatch")

    def test_standard_dc_and_local_schema(self):
        t=self.tree("verb=GetRecord&metadataPrefix=oai_dc&identifier=oai:campus:p1")
        self.assertIsNotNone(t.find(".//{http://purl.org/dc/elements/1.1/}title"))
        s,h,b=self.request("/research.xsd");self.assertEqual(s,200)
        self.assertEqual(E.fromstring(b).get("targetNamespace"),"urn:campus:research")
