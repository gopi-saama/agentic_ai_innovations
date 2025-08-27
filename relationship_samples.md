# PubMed Knowledge Graph Relationship Samples

## author_of_rels.csv
```
startNode,endNode
Paper_13553038,Author_97952a7ab4
Paper_13553039,Author_198399d345
Paper_13553040,Author_8d4d18a691
Paper_13553041,Author_ecdfff8572
Paper_13553042,Author_0df0967c1b
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## authored_rels.csv
```
startNode,endNode
Author_97952a7ab4,Paper_13553038
Author_198399d345,Paper_13553039
Author_8d4d18a691,Paper_13553040
Author_ecdfff8572,Paper_13553041
Author_0df0967c1b,Paper_13553042
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## chemical_in_rels.csv
```
startNode,endNode
Chemical_D010969,Paper_13553042
Chemical_D013778,Paper_13553051
Chemical_C011314,Paper_13553051
Chemical_C009591,Paper_13553051
Chemical_D010088,Paper_13553051
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## cited_by_rels.csv
```
startNode,endNode,citation_text
Paper_20992043,Paper_13553711,Am J Physiol. 1946 Jul 1;146:610-2
Paper_16994235,Paper_13553711,J Physiol. 1931 Oct 22;73(2):163-83
Paper_16694475,Paper_13553711,J Clin Invest. 1937 Mar;16(2):245-55
Paper_14808861,Paper_13553711,Rev Esp Fisiol. 1950 Sep;6(3):143-56
Paper_14781516,Paper_13553711,Rev Esp Fisiol. 1950 Jun;6(2):131-42
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## cites_rels.csv
```
startNode,endNode,citation_text
Paper_13553711,Paper_20992043,Am J Physiol. 1946 Jul 1;146:610-2
Paper_13553711,Paper_16994235,J Physiol. 1931 Oct 22;73(2):163-83
Paper_13553711,Paper_16694475,J Clin Invest. 1937 Mar;16(2):245-55
Paper_13553711,Paper_14808861,Rev Esp Fisiol. 1950 Sep;6(3):143-56
Paper_13553711,Paper_14781516,Rev Esp Fisiol. 1950 Jun;6(2):131-42
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## contains_chemical_rels.csv
```
startNode,endNode
Paper_13553042,Chemical_D010969
Paper_13553051,Chemical_D013778
Paper_13553051,Chemical_C011314
Paper_13553051,Chemical_C009591
Paper_13553051,Chemical_D010088
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## funded_by_rels.csv
```
startNode,endNode
Paper_9481778,Grant_United Kingdom_Wellcome Trust
Paper_9481782,Grant_United Kingdom_Wellcome Trust
Paper_9481786,Grant_HD-01095
Paper_9481792,Grant_1RO1-HD-26492-02
Paper_9481798,Grant_R01DC00303
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## funds_rels.csv
```
startNode,endNode
Grant_United Kingdom_Wellcome Trust,Paper_9481778
Grant_United Kingdom_Wellcome Trust,Paper_9481782
Grant_HD-01095,Paper_9481786
Grant_1RO1-HD-26492-02,Paper_9481792
Grant_R01DC00303,Paper_9481798
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## has_keyword_rels.csv
```
startNode,endNode
Paper_13553038,Keyword_a2cbfb0c20
Paper_13553039,Keyword_0c6de9c58c
Paper_13553040,Keyword_9173ac8f85
Paper_13553041,Keyword_2dad246042
Paper_13553041,Keyword_a3e267b930
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## has_mesh_term_rels.csv
```
startNode,endNode
Paper_13553038,MeshTerm_D000038
Paper_13553038,MeshTerm_D001921
Paper_13553038,MeshTerm_D001922
Paper_13553038,MeshTerm_D006801
Paper_13553038,MeshTerm_D011379
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## has_publication_type_rels.csv
```
startNode,endNode
Paper_13553038,PublicationType_D016428
Paper_13553039,PublicationType_D016428
Paper_13553040,PublicationType_D016428
Paper_13553041,PublicationType_D016428
Paper_13553042,PublicationType_D016428
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## keyword_of_rels.csv
```
startNode,endNode
Keyword_a2cbfb0c20,Paper_13553038
Keyword_0c6de9c58c,Paper_13553039
Keyword_9173ac8f85,Paper_13553040
Keyword_2dad246042,Paper_13553041
Keyword_a3e267b930,Paper_13553041
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## mesh_term_of_rels.csv
```
startNode,endNode
MeshTerm_D000038,Paper_13553038
MeshTerm_D001921,Paper_13553038
MeshTerm_D001922,Paper_13553038
MeshTerm_D006801,Paper_13553038
MeshTerm_D011379,Paper_13553038
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## origin_of_rels.csv
```
startNode,endNode
Country_72ddd2b619,Paper_13553038
Country_72ddd2b619,Paper_13553039
Country_72ddd2b619,Paper_13553040
Country_72ddd2b619,Paper_13553041
Country_72ddd2b619,Paper_13553042
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## publication_type_of_rels.csv
```
startNode,endNode
PublicationType_D016428,Paper_13553038
PublicationType_D016428,Paper_13553039
PublicationType_D016428,Paper_13553040
PublicationType_D016428,Paper_13553041
PublicationType_D016428,Paper_13553042
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## published_from_rels.csv
```
startNode,endNode
Paper_13553038,Country_72ddd2b619
Paper_13553039,Country_72ddd2b619
Paper_13553040,Country_72ddd2b619
Paper_13553041,Country_72ddd2b619
Paper_13553042,Country_72ddd2b619
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## published_in_rels.csv
```
startNode,endNode
Paper_13553038,Journal_0401001
Paper_13553039,Journal_0401001
Paper_13553040,Journal_0401001
Paper_13553041,Journal_0401001
Paper_13553042,Journal_0401001
```

<!-- =============== RELATIONSHIP SEPARATOR =============== -->

## publishes_rels.csv
```
startNode,endNode
Journal_0401001,Paper_13553038
Journal_0401001,Paper_13553039
Journal_0401001,Paper_13553040
Journal_0401001,Paper_13553041
Journal_0401001,Paper_13553042
```


<!-- //1. Author and Paper Relationships
//Cypher

// Import AUTHOR_OF relationships
LOAD CSV WITH HEADERS FROM 'file:///author_of_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS authorId
MATCH (p:Paper {pmid: pmid})
MATCH (a:Author {id: authorId})
CREATE (p)-[:AUTHOR_OF]->(a);

// Import AUTHORED relationships
LOAD CSV WITH HEADERS FROM 'file:///authored_rels.csv' AS row
WITH row.startNode AS authorId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (a:Author {id: authorId})
MATCH (p:Paper {pmid: pmid})
CREATE (a)-[:AUTHORED]->(p);

//2. Chemical and Paper Relationships
//Cypher

// Import CHEMICAL_IN relationships
LOAD CSV WITH HEADERS FROM 'file:///chemical_in_rels.csv' AS row
WITH row.startNode AS chemicalId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (c:Chemical {id: chemicalId})
MATCH (p:Paper {pmid: pmid})
CREATE (c)-[:CHEMICAL_IN]->(p);

// Import CONTAINS_CHEMICAL relationships
LOAD CSV WITH HEADERS FROM 'file:///contains_chemical_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS chemicalId
MATCH (p:Paper {pmid: pmid})
MATCH (c:Chemical {id: chemicalId})
CREATE (p)-[:CONTAINS_CHEMICAL]->(c);

//3. Citation Relationships
//Cypher

// Import CITED_BY relationships
LOAD CSV WITH HEADERS FROM 'file:///cited_by_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS startPmid, toInteger(split(row.endNode, '_')[1]) AS endPmid, row.citation_text AS citation
MATCH (p1:Paper {pmid: startPmid})
MATCH (p2:Paper {pmid: endPmid})
CREATE (p1)-[:CITED_BY {citation_text: citation}]->(p2);

// Import CITES relationships
LOAD CSV WITH HEADERS FROM 'file:///cites_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS startPmid, toInteger(split(row.endNode, '_')[1]) AS endPmid, row.citation_text AS citation
MATCH (p1:Paper {pmid: startPmid})
MATCH (p2:Paper {pmid: endPmid})
CREATE (p1)-[:CITES {citation_text: citation}]->(p2);

//4. Grant and Paper Relationships
//Cypher

// Import FUNDED_BY relationships
LOAD CSV WITH HEADERS FROM 'file:///funded_by_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS grantId
MATCH (p:Paper {pmid: pmid})
MATCH (g:Grant {id: grantId})
CREATE (p)-[:FUNDED_BY]->(g);

// Import FUNDS relationships
LOAD CSV WITH HEADERS FROM 'file:///funds_rels.csv' AS row
WITH row.startNode AS grantId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (g:Grant {id: grantId})
MATCH (p:Paper {pmid: pmid})
CREATE (g)-[:FUNDS]->(p);

//5. Keyword and Paper Relationships
Cypher

// Import HAS_KEYWORD relationships
LOAD CSV WITH HEADERS FROM 'file:///has_keyword_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS keywordId
MATCH (p:Paper {pmid: pmid})
MATCH (k:Keyword {id: keywordId})
CREATE (p)-[:HAS_KEYWORD]->(k);

// Import KEYWORD_OF relationships
LOAD CSV WITH HEADERS FROM 'file:///keyword_of_rels.csv' AS row
WITH row.startNode AS keywordId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (k:Keyword {id: keywordId})
MATCH (p:Paper {pmid: pmid})
CREATE (k)-[:KEYWORD_OF]->(p);

//6. MeshTerm and Paper Relationships
Cypher

// Import HAS_MESH_TERM relationships
LOAD CSV WITH HEADERS FROM 'file:///has_mesh_term_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS meshId
MATCH (p:Paper {pmid: pmid})
MATCH (m:MeshTerm {id: meshId})
CREATE (p)-[:HAS_MESH_TERM]->(m);

// Import MESH_TERM_OF relationships
LOAD CSV WITH HEADERS FROM 'file:///mesh_term_of_rels.csv' AS row
WITH row.startNode AS meshId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (m:MeshTerm {id: meshId})
MATCH (p:Paper {pmid: pmid})
CREATE (m)-[:MESH_TERM_OF]->(p);

//7. Publication and Paper Relationships
//Cypher

// Import HAS_PUBLICATION_TYPE relationships
LOAD CSV WITH HEADERS FROM 'file:///has_publication_type_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS publicationId
MATCH (p:Paper {pmid: pmid})
MATCH (pub:Publication {id: publicationId})
CREATE (p)-[:HAS_PUBLICATION_TYPE]->(pub);

// Import PUBLICATION_TYPE_OF relationships
LOAD CSV WITH HEADERS FROM 'file:///publication_type_of_rels.csv' AS row
WITH row.startNode AS publicationId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (pub:Publication {id: publicationId})
MATCH (p:Paper {pmid: pmid})
CREATE (pub)-[:PUBLICATION_TYPE_OF]->(p);

//8. Country and Paper Relationships
//Cypher

// Import PUBLISHED_FROM relationships
LOAD CSV WITH HEADERS FROM 'file:///published_from_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS countryId
MATCH (p:Paper {pmid: pmid})
MATCH (c:Country {id: countryId})
CREATE (p)-[:PUBLISHED_FROM]->(c);

// Import ORIGIN_OF relationships
LOAD CSV WITH HEADERS FROM 'file:///origin_of_rels.csv' AS row
WITH row.startNode AS countryId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (c:Country {id: countryId})
MATCH (p:Paper {pmid: pmid})
CREATE (c)-[:ORIGIN_OF]->(p);

//9. Journal and Paper Relationships
//Cypher

// Import PUBLISHED_IN relationships
LOAD CSV WITH HEADERS FROM 'file:///published_in_rels.csv' AS row
WITH toInteger(split(row.startNode, '_')[1]) AS pmid, row.endNode AS journalId
MATCH (p:Paper {pmid: pmid})
MATCH (j:Journal {id: journalId})
CREATE (p)-[:PUBLISHED_IN]->(j);

// Import PUBLISHES relationships
LOAD CSV WITH HEADERS FROM 'file:///publishes_rels.csv' AS row
WITH row.startNode AS journalId, toInteger(split(row.endNode, '_')[1]) AS pmid
MATCH (j:Journal {id: journalId})
MATCH (p:Paper {pmid: pmid})
CREATE (j)-[:PUBLISHES]->(p); -->