-- Triage requires integer column named 'entity_id' as a unique identifier
CREATE TABLE raw.bill_id_mapping (
	entity_id SERIAL,
	bill_id varchar
)

