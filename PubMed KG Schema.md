Knowledge Graph Schema
This blueprint defines the data types and value structures for the nodes and relationships in your knowledge graph. The schema is designed to be flexible and highly interconnected for powerful querying and analysis.

1. Node Blueprints
Each node type has a specific set of properties with defined data types.

Paper Node : This node represents a single publication. The pmid is the unique identifier.

- pmid: String (Primary Key)

- title: String

- abstract: String

- pubdate: Date

- doi: String

- pages: String

- issue: String

- languages: String

Author Node : This node represents a person who has authored a paper.

- name: String (Unique Identifier)

MeshTerm Node : This node represents a Medical Subject Heading.

- term: String

- mesh_id: String (Unique Identifier)

PublicationType Node : This node categorizes the publication type.

- type: String

- type_id: String (Unique Identifier)

Chemical Node : This node represents a chemical substance.

- name: String

- chemical_id: String (Unique Identifier)

Keyword Node : This node represents a keyword associated with a paper.

- keyword: String (Unique Identifier)

Grant Node : This node stores information about research grants.

- grant_id: String (Unique Identifier)

- grant_acronym: String

- agency: String

Journal Node : This new node represents a journal.

- name: String (Unique Identifier)

- nlm_unique_id: String

- issn_linking: String

- medline_ta: String

Country Node : This new node represents a country.

- name: String (Unique Identifier)

2. Relationship Blueprints
Relationships define the connections between nodes. They are directional and can have properties.

WROTE Relationship : 

- Direction: Author → Paper

- Properties: None

HAS_MESH_TERM Relationship : 

- Direction: Paper → MeshTerm

- Properties: None

HAS_PUBLICATION_TYPE Relationship : 

- Direction: Paper → PublicationType

- Properties: None

CONTAINS_CHEMICAL Relationship : 

- Direction: Paper → Chemical

- Properties: None

HAS_KEYWORD Relationship : 

- Direction: Paper → Keyword

- Properties: None

CITES Relationship : 

- Direction: Paper (citing) → Paper (cited)

- Properties:

    - citation_text: String

FUNDED_BY Relationship : 

- Direction: Paper → Grant

- Properties: None

PUBLISHED_IN Relationship : 

- Direction: Paper → Journal

- Properties: None

PUBLISHED_FROM Relationship : 

- Direction: Paper → Country

- Properties: None