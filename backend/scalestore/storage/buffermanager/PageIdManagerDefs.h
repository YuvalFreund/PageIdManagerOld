//
// Created by YuvalFreund on 08.02.24.
//

#ifndef SCALESTOREDB_PAGEIDMANAGERDEFS_H
#define SCALESTOREDB_PAGEIDMANAGERDEFS_H


#define INVALID_SSD_SLOT 0xFFFFFFFFFFFFFFFF
#define INVALID_NODE_ID 0xFFFFFFFFFFFFFFFF
#define PARTITION_MASK 0x000000000000FFFF
#define SSD_SLOT_MASK 0X0000FFFFFFFFFFFF
#define PAGE_DIRECTORY_NEGATIVE_MASK 0x00FFFFFFFFFFFFFF
#define CACHED_DIRECTORY_MASK 0xFF00000000000000
#define CREATED_AT_NODE_0 1
#define CREATED_AT_NODE_1 2
#define CONSISTENT_HASHING_WEIGHT 3
#define SSD_PID_MAPS_AMOUNT 65536


#endif //SCALESTOREDB_PAGEIDMANAGERDEFS_H
