# Objective

PodWeaver will run on each user's system as a standalone application that can handle all incoming and outgoing data traffic for file transfer. It must ensure easy and quick peer discovery, regular checks to verify peer fairness and security against malicious peers.

## Implementation Plan

PodWeaver will be developed in several phases, each phase will be tested individually before an integration step to combine the implemented sub-modules into the final product.

Each module and sub-module will function independent of any sibling unit.

## 1: File Chunking

In case of a seeder, the user provided file to the PodWeaver application is subdivided into units of invariant length. This invariant length is established through a piece division algorithm that ensures total piece count remains manageable while balancing size of an inidividual piece.

### 1.1: File Division

## 2: Connection Establishment

Phase 1 involves setting up a TCP connection between two peers and testing file transfer between them.

### 2.1: 

