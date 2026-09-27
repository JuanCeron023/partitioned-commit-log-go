export interface PartitionInfo {
  partition: number;
  end_offset: number;
}

export interface TopicInfo {
  name: string;
  partitions: PartitionInfo[];
}

export interface GroupLag {
  group: string;
  topic: string;
  partition: number;
  committed: number;
  end_offset: number;
  lag: number;
}

export interface Stats {
  produced: number;
  topics: TopicInfo[];
  lags: GroupLag[];
}

export interface Message {
  key?: string;
  value: string; // base64
  partition: number;
  offset: number;
}
