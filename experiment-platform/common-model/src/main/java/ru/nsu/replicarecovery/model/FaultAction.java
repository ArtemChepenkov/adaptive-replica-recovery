package ru.nsu.replicarecovery.model;

public enum FaultAction {
  STOP_BROKER,
  KILL_BROKER,
  RESTART_BROKER,
  ISOLATE_BROKER,
  ISOLATE_DOMAIN,
  LIMIT_NETWORK,
  ADD_LATENCY,
  ADD_PACKET_LOSS,
  LIMIT_DISK
}

