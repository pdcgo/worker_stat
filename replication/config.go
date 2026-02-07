package replication

type ReplicationConfig struct {
	SlotName        string `json:"slot_name"`
	SlotTemporary   bool   `json:"slot_temporary"`
	PublicationName string `json:"publication_name"`
}
