package assembly

/* TODO: Rewrite, now after pluggable feature addition

The assembly package is the only place with knowledge on how to map abstract and logical ETL model
concepts into physical stream entities, with two areas of responsibility:

	1. Config: Managing entity client lifecycles
	2. Entity Factory: Creating executable ETL Stream Entities based on Stream ETL Spec contents,
       as provided by the StreamBuilder.

*/
