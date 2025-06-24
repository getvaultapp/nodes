package main

import (
	"archive/zip"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/getvaultapp/storage-engine/vault-storage-engine/pkg/bucket"
	"github.com/getvaultapp/storage-engine/vault-storage-engine/pkg/config"
	"github.com/getvaultapp/storage-engine/vault-storage-engine/pkg/database"
	"github.com/getvaultapp/storage-engine/vault-storage-engine/pkg/datastorage"
	"github.com/getvaultapp/storage-engine/vault-storage-engine/pkg/sharding"
	"github.com/getvaultapp/storage-engine/vault-storage-engine/pkg/utils"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

// For discovery and p2p communication
type NodeInfo struct {
	NodeID   string `json:"node_id"`
	NodeType string `json:"node_type"`
	Address  string `json:"address"`
	LastSeen int64  `json:"last_seen"`
	Hash     uint32 `json:"-"`
}

// A peer represents an individual participator node on the network
type Peer struct {
	NodeID         string            `json:"node_id"`
	Address        string            `json:"address"`
	NodeType       string            `json:"node_type"`
	Capabilities   map[string]string `json:"capabilities"`
	LastHeartbeat  time.Time         `json:"last_heartbeat"`
	AvailableSpace int64             `json:"available_space,omitempty"`
}

// Global Time to get CPU usage
var startTime = time.Now()

// Simple Incentives Design
var (
	totalVaultTokens float64
	tokenLock        sync.Mutex
)

const (
	tokensPerTask          = 10.0  //Token reward per task
	tokensPerMBProcessed   = 0.1   // Token reward per MB processed
	tokenPerReconstruction = 20.00 // Token reward per reconstruction
)

var (
	nodeRegistry = make(map[string]NodeInfo)
	registryLock sync.RWMutex
	peerList     []Peer
	peerLock     sync.RWMutex
)

var (
	taskQueue   = make(map[string]PendingTask)
	taskQueueMu sync.Mutex
	myNodeID    = os.Getenv("NODE_ID")
)

var (
	totalTasksProcessed int
	totalDataProcessed  uint64 // in bytes
	totalTaskDuration   time.Duration
	metricsLock         sync.Mutex
)

func pingPong(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"message": "pong"})
}

// GetMemoryUsage returns the memory usage of the current process
func GetMemoryUsage() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return memStats.Alloc // Memory allocated in bytes
}

func GetCPUUsage() (float64, error) {
	var usage syscall.Rusage
	err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	if err != nil {
		return 0, fmt.Errorf("failed to get CPU usage, %v", err)
	}

	// Convert the construction node's CPU time to seconds
	userTime := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6
	systemTime := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6

	// Get total CPU time by the process
	totalCPUTime := userTime + systemTime

	elaspedTime := time.Since(startTime).Seconds()

	cpuUsage := (totalCPUTime / elaspedTime) * 100
	return cpuUsage, nil
}

// registerHandler is responsible for handling node discovery-based registration
func registerHandler(w http.ResponseWriter, r *http.Request) {
	var node NodeInfo
	json.NewDecoder(r.Body).Decode(&node)
	node.LastSeen = time.Now().Unix()
	registryLock.Lock()
	nodeRegistry[node.NodeID] = node
	registryLock.Unlock()
	json.NewEncoder(w).Encode(map[string]string{"status": "registered"})
}

// nodeHandler provides a list of actively participating and recheachable nodes on the network
func nodesHandler(w http.ResponseWriter, r *http.Request) {
	registryLock.RLock()
	var nodes []NodeInfo
	for _, node := range nodeRegistry {
		nodes = append(nodes, node)
	}
	registryLock.RUnlock()
	json.NewEncoder(w).Encode(nodes)
}

// GossipRegisterHandler prepares the nodes for communication on the network
func gossipRegisterHandler(w http.ResponseWriter, r *http.Request) {
	var p Peer
	if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	peerLock.Lock()
	for _, existing := range peerList {
		if existing.NodeID == p.NodeID {
			peerLock.Unlock()
			return
		}
	}
	peerList = append(peerList, p)
	peerLock.Unlock()
	w.WriteHeader(http.StatusOK)
}

func gossipListHandler(w http.ResponseWriter, r *http.Request) {
	peerLock.RLock()
	json.NewEncoder(w).Encode(peerList)
	peerLock.RUnlock()
}

func startHealthCheck() {
	var BASE_URL string = os.Getenv("BASE_URL")
	if BASE_URL == "" {
		BASE_URL = "http://localhost"
	}
	go func() {
		for {
			time.Sleep(30 * time.Second)
			nodeInfo := map[string]interface{}{
				"node_id":   myNodeID,
				"node_type": "construction",
				"address":   fmt.Sprintf("%s:%s", BASE_URL, os.Getenv("CONSTRUCTION_PORT")),
				"time":      time.Now().Format(time.RFC3339),
			}
			jsonData, _ := json.Marshal(nodeInfo)
			http.Post(BASE_URL+":"+os.Getenv("DISCOVERY_PORT")+"/register", "application/json", bytes.NewReader(jsonData))
		}
	}()
}

func startGossip() {
	go func() {
		for {
			time.Sleep(10 * time.Second)
			peerLock.RLock()
			if len(peerList) == 0 {
				peerLock.RUnlock()
				continue
			}
			target := peerList[rand.Intn(len(peerList))]
			peerLock.RUnlock()

			url := target.Address + "/gossip/peers"
			resp, err := http.Get(url)
			if err != nil {
				continue
			}
			var remotePeers []Peer
			json.NewDecoder(resp.Body).Decode(&remotePeers)
			resp.Body.Close()

			peerLock.Lock()
			for _, rp := range remotePeers {
				found := false
				for _, p := range peerList {
					if p.NodeID == rp.NodeID {
						found = true
						break
					}
				}
				if !found && len(peerList) < 50 {
					peerList = append(peerList, rp)
				}
			}
			peerLock.Unlock()
		}
	}()
}

func startDiscoveryAndP2P() {
	r := mux.NewRouter()
	r.HandleFunc("/register", registerHandler)
	r.HandleFunc("/nodes", nodesHandler)
	r.HandleFunc("/gossip/register", gossipRegisterHandler)
	r.HandleFunc("/gossip/peers", gossipListHandler)
	r.HandleFunc("/lookup/storage", handleStorageLookup).Methods("GET")
	r.HandleFunc("/lookup/construction", handleConstructionLookup).Methods("GET")

	startHealthCheck()
	startGossip()

	port := os.Getenv("DISCOVERY_PORT")
	if port == "" {
		port = "9100"
	}

	go func() {
		log.Printf("Starting embedded discovery + gossip server on port %s...\n", port)
		srv := &http.Server{
			Addr:    ":" + port,
			Handler: r,
			//TLSConfig: tlsConfig,
		}
		log.Fatal(srv.ListenAndServe())
	}()
}

// PendingTask represents a file processing task.
type PendingTask struct {
	BuucketID string
	ObjectID  string
	VersionID string
	Data      []byte
	FileName  string
	CreatedAt time.Time
	Assigned  bool
}

func registerTask(bucketID, objectID, versionID, fileName string, data []byte) {
	taskQueueMu.Lock()
	defer taskQueueMu.Unlock()
	taskQueue[objectID] = PendingTask{
		BuucketID: bucketID,
		ObjectID:  objectID,
		VersionID: versionID,
		Data:      data,
		FileName:  fileName,
		CreatedAt: time.Now(),
		Assigned:  false,
	}
}

func claimTask(objectID string) (PendingTask, bool) {
	taskQueueMu.Lock()
	defer taskQueueMu.Unlock()
	task, exists := taskQueue[objectID]
	if exists && !task.Assigned {
		task.Assigned = true
		taskQueue[objectID] = task
		return task, true
	}
	return PendingTask{}, false
}

func handleShardInfo(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	var req struct {
		BucketID  string `json:"bucket_id"`
		ObjectID  string `json:"object_id"`
		VersionID string `json:"version_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.BucketID == "" || req.ObjectID == "" || req.VersionID == "" {
		http.Error(w, "Missing required fields: bucket_id, object_id, or version_id", http.StatusBadRequest)
		return
	}

	// Query the database for shard locations
	rows, err := db.Query(`
        SELECT shard_index, storage_node_id, storage_node_address
        FROM shard_locations
        WHERE bucket_id = ? AND object_id = ? AND version_id = ?
    `, req.BucketID, req.ObjectID, req.VersionID)
	if err != nil {
		http.Error(w, "Failed to query shard locations", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var shards []map[string]interface{}
	for rows.Next() {
		var shardIndex int
		var storageNodeID, storageNodeAddress string
		if err := rows.Scan(&shardIndex, &storageNodeID, &storageNodeAddress); err != nil {
			http.Error(w, "Failed to scan shard data", http.StatusInternalServerError)
			return
		}

		shards = append(shards, map[string]interface{}{
			"shard_index":          shardIndex,
			"storage_node_id":      storageNodeID,
			"storage_node_address": storageNodeAddress,
		})
	}

	if len(shards) == 0 {
		http.Error(w, "No shard information found for the specified file version", http.StatusNotFound)
		return
	}

	// Return the shard information
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"shards": shards,
	})
}

func registerWithDiscovery(nodeID, discoveryURL, selfAddress string) {
	regURL := discoveryURL + "/register"
	payload := map[string]string{
		"node_id":   nodeID,
		"node_type": "construction",
		"address":   selfAddress,
	}
	data, _ := json.Marshal(payload)
	client := &http.Client{}

	var resp *http.Response
	var err error
	for attempt := 1; attempt <= 5; attempt++ {
		resp, err = client.Post(regURL, "application/json", bytes.NewReader(data))
		if err == nil {
			break
		}
		log.Printf("Discovery registration failed (attempt %d): %v", attempt, err)
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	if err != nil {
		log.Printf("Final failure registering with discovery after retries: %v", err)
		return
	}
	defer resp.Body.Close()
	log.Printf("Node %s registered with discovery", nodeID)
}

// HTTP handlers for construction node.
// If the node is active then it is okay
func handleHealth(w http.ResponseWriter, r *http.Request) {
	// TODO let's get more advanced means of checking the functionality of a construction node
	info := map[string]string{
		"node_id":   os.Getenv("NODE_ID"),
		"node_type": os.Getenv("NODE_TYPE"),
		"location":  "earth", // Implement node geolocation
		"status":    "active",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func handleInfo(w http.ResponseWriter, r *http.Request) {
	info := map[string]string{
		"node_id":   os.Getenv("NODE_ID"),
		"node_type": os.Getenv("NODE_TYPE"),
		"time":      time.Now().Format(time.RFC3339),
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func handleProcessFile(w http.ResponseWriter, r *http.Request, db *sql.DB, store sharding.ShardStore, cfg *config.Config, logger *zap.Logger) {
	startTime := time.Now()

	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read file", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Get the bucketID and filename from the request
	// ObjectID and it's versionID is generated for every new file
	bucketID := r.Header.Get("X-Bucket-ID")
	objectID := r.Header.Get("X-Object-ID")
	fileName := r.Header.Get("X-Filename")
	if bucketID == "" || fileName == "" {
		http.Error(w, "Missing required headers", http.StatusBadRequest)
		return
	}

	// A new object is created if the object ID is not provided
	// Else we create a new version of the object instead
	if objectID == "" {
		objectID = uuid.New().String()
	}

	versionID := uuid.New().String()

	// Register the task in our queue
	registerTask(bucketID, objectID, versionID, fileName, data)

	// Instead of immediately processing, we could implement a work queue
	// But for now, let's claim and process the task immediately
	task, ok := claimTask(objectID)
	if !ok {
		http.Error(w, "Task already assigned", http.StatusConflict)
		return
	}

	// Process the task asynchronously
	go func() {
		// Use the new distributed storage functionality
		_, shardLocations, _, err := datastorage.NewStoreData(
			db,
			task.Data,
			task.BuucketID,
			task.ObjectID,
			task.VersionID,
			task.FileName,
			store,
			cfg,
			[]string{}, // This is not used anymore as nodes are discovered dynamically
			logger,
		)

		if err != nil {
			logger.Error("Failed to store data", zap.Error(err))
			return
		}

		if objectID == "" {
			// Different message if a new object was created
			logger.Info("Object stored successfully",
				zap.String("object_id", task.ObjectID),
				zap.String("version_id", task.VersionID),
				zap.Any("shard_locations", shardLocations))
		} else {
			// Different message if the object was updated instead of recreated
			logger.Info("Object updated successfully",
				zap.String("version_id", task.VersionID),
				zap.Any("shard_locations", shardLocations))
		}
	}()

	// Update processing metrics
	metricsLock.Lock()
	totalTasksProcessed++
	totalDataProcessed += uint64(len(data))
	totalTaskDuration += time.Since((startTime))
	metricsLock.Unlock()

	// Reward tokens for processing, simply update the amount of tokens for every task
	tokenLock.Lock()
	totalVaultTokens += tokensPerTask
	totalVaultTokens += float64(len(data)) / (1024 * 1024) * tokensPerMBProcessed // Reward based on MB processed
	tokenLock.Unlock()

	response := map[string]string{
		"object_id":  objectID,
		"version_id": versionID,
		"status":     "data sharded and fowarded",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// This should correctly list all versions of the object_id and bucket_id from the console of the construction node
func handleListVersions(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	//bucketID := r.URL.Query().Get("bucket_id")
	//objectID := r.URL.Query().Get("object_id")

	var req struct {
		BucketID string `json:"bucket_id"`
		ObjectID string `json:"object_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	if req.BucketID == "" || req.ObjectID == "" {
		http.Error(w, "Missing bucket_id or object_id", http.StatusBadRequest)
		return
	}

	rows, err := db.Query(`
		SELECT version_id, created_at, shard_locations
		FROM versions
		WHERE bucket_id = ? AND object_id = ?
		ORDER BY created_at DESC
	`, req.BucketID, req.ObjectID)
	if err != nil {
		http.Error(w, "DB error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var versions []map[string]interface{}
	for rows.Next() {
		var versionID, createdAt, shardLocations string
		if err := rows.Scan(&versionID, &createdAt, &shardLocations); err != nil {
			continue
		}
		var loc map[string]string
		_ = json.Unmarshal([]byte(shardLocations), &loc)

		versions = append(versions, map[string]interface{}{
			"version_id":      versionID,
			"created_at":      createdAt,
			"shard_locations": loc,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(versions)
}

func handleDeleteFileFromStorage(w http.ResponseWriter, r *http.Request, db *sql.DB, store sharding.ShardStore, cfg *config.Config, logger *zap.Logger) {
	var req struct {
		BucketID  string `json:"bucket_id"`
		ObjectID  string `json:"object_id"`
		VersionID string `json:"version_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err := datastorage.DeleteVersionShardsAcrossNodes(req.BucketID, req.ObjectID, req.VersionID, store, cfg, logger)
	if err != nil {
		http.Error(w, "Failed to delete shards across all storage nodes", http.StatusInternalServerError)
		logger.Error("Failed to delete shards across all storage nodes", zap.Error(err))
		return
	}

	err = bucket.DeleteObjectByVersion(db, req.BucketID, req.ObjectID, req.VersionID)
	if err != nil {
		http.Error(w, "Failed to delete object version from database", http.StatusInternalServerError)
		logger.Error("Failed to delete object version from database", zap.Error(err))
		return
	}

	response := map[string]string{
		"object_id":  req.ObjectID,
		"version_id": req.VersionID,
		"status":     "deleting shards from storage",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleDeleteAllFileVersionsFromStorage(w http.ResponseWriter, r *http.Request, db *sql.DB, store sharding.ShardStore, cfg *config.Config, logger *zap.Logger) {
	var req struct {
		BucketID string `json:"bucket_id"`
		ObjectID string `json:"object_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err := datastorage.DeleteAllVersionShardsAcrossNodes(req.BucketID, req.ObjectID, store, cfg, logger)
	if err != nil {
		http.Error(w, "Failed to delete all shards versions across all storage nodes", http.StatusInternalServerError)
		logger.Error("Failed to delete all shards versions across all storage nodes", zap.Error(err))
		return
	}

	err = bucket.DeleteObject(db, req.BucketID, req.ObjectID)
	if err != nil {
		http.Error(w, "Failed to delete object from database", http.StatusInternalServerError)
		logger.Error("Failed to delete object from database", zap.Error(err))
		return
	}

	response := map[string]string{
		"object_id": req.ObjectID,
		"status":    "deleting shards from storage",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func handleReconstructFile(w http.ResponseWriter, r *http.Request, db *sql.DB, store sharding.ShardStore, cfg *config.Config, logger *zap.Logger) {
	startTime := time.Now()

	var req struct {
		BucketID  string `json:"bucket_id"`
		ObjectID  string `json:"object_id"`
		VersionID string `json:"version_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		log.Printf("%v", err)
		return
	}
	defer r.Body.Close()

	// Use the new distributed retrieve function
	data, fileName, err := datastorage.NewRetrieveData(
		db,
		req.BucketID,
		req.ObjectID,
		req.VersionID,
		store,
		cfg,
		logger,
	)

	if err != nil {
		logger.Error("Reconstruction failed", zap.Error(err))
		http.Error(w, fmt.Sprintf("Reconstruction failed: %v", err), http.StatusInternalServerError)
		return
	}

	buf := new(bytes.Buffer)
	zipWriter := zip.NewWriter(buf)
	f, err := zipWriter.Create(fileName)
	if err != nil {
		http.Error(w, "Failed to create ZIP file", http.StatusInternalServerError)
		return
	}
	_, err = f.Write(data)
	if err != nil {
		http.Error(w, "Failed to write file to ZIP", http.StatusInternalServerError)
		return
	}

	err = zipWriter.Close()
	if err != nil {
		http.Error(w, "Failed to close ZIP file", http.StatusInternalServerError)
		return
	}

	// Update reconstruction metrics
	metricsLock.Lock()
	totalTasksProcessed++
	totalTaskDuration += time.Since(startTime)
	metricsLock.Unlock()

	// Reward tokens for reconstruction
	tokenLock.Lock()
	totalVaultTokens += tokenPerReconstruction
	tokenLock.Unlock()

	zipFileName := fmt.Sprintf("%s-v(%s).zip", req.ObjectID, req.VersionID)
	w.Header().Set("Content-Type", "application/zip")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"%s\"", zipFileName))
	w.Write(buf.Bytes())
}

func handleTokenBalance(w http.ResponseWriter, r *http.Request) {
	tokenLock.Lock()
	defer tokenLock.Unlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(
		map[string]float64{
			"vault_tokens": totalVaultTokens,
		})
}

// This should work along with the discovery, not the construction port
func handleStorageLookup(w http.ResponseWriter, r *http.Request) {
	storageNodes := []map[string]string{}

	peerLock.RLock()
	for _, p := range peerList {
		if p.NodeType == "storage" {
			storageNodes = append(storageNodes, map[string]string{
				"address": p.Address,
			})
		}
	}
	peerLock.RUnlock()

	// Fallback to nodeRegistry if peerList is empty
	if len(storageNodes) == 0 {
		registryLock.RLock()
		for _, n := range nodeRegistry {
			if n.NodeType == "storage" {
				storageNodes = append(storageNodes, map[string]string{
					"address": n.Address,
				})
			}
		}
		registryLock.RUnlock()
	}

	if len(storageNodes) == 0 {
		http.Error(w, "no storage nodes available", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(storageNodes)
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	metricsLock.Lock()
	defer metricsLock.Unlock()

	w.Header().Set("Content-Type", "application/json")

	if totalTasksProcessed <= 0 {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"total_tasks_processed": totalTasksProcessed,
			"total_data_processed":  totalDataProcessed, // in bytes
			"average_task_duration": 0,
		})
	} else {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"total_tasks_processed": totalTasksProcessed,
			"total_data_processed":  totalDataProcessed, // in bytes
			"average_task_duration": totalTaskDuration / time.Duration(totalTasksProcessed),
		})
	}
}

func handleConstructionLookup(w http.ResponseWriter, r *http.Request) {
	log.Println("Handling /lookup/construction request")

	constructionNodes := []map[string]string{}

	peerLock.RLock()
	//log.Printf("Peer list: %+v", peerList)
	for _, p := range peerList {
		if p.NodeType == "construction" {
			constructionNodes = append(constructionNodes, map[string]string{
				"address": p.Address,
			})
		}
	}
	peerLock.RUnlock()

	if len(constructionNodes) == 0 {
		registryLock.RLock()
		log.Printf("Node registry: %+v", nodeRegistry)
		for _, n := range nodeRegistry {
			if n.NodeType == "construction" {
				constructionNodes = append(constructionNodes, map[string]string{
					"address": n.Address,
				})
			}
		}
		registryLock.RUnlock()
	}

	if len(constructionNodes) == 0 {
		log.Println("No construction nodes available")
		http.Error(w, "no construction nodes available", http.StatusServiceUnavailable)
		return
	}

	log.Printf("Returning construction nodes: %+v", constructionNodes)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(constructionNodes)
}

func registerAllDiscoveredPeersFromRegistry() {
	var BASE_URL string = os.Getenv("BASE_URL")
	if BASE_URL == "" {
		BASE_URL = "http://localhost"
	}
	resp, err := http.Get(BASE_URL + ":" + os.Getenv("DISCOVERY_PORT") + "/nodes")
	if err != nil {
		log.Printf("Failed to fetch /nodes: %v", err)
		return
	}
	defer resp.Body.Close()

	var nodes []NodeInfo
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		log.Printf("Failed to decode /nodes: %v", err)
		return
	}

	for _, node := range nodes {
		if node.NodeType != "storage" || node.NodeID == myNodeID {
			continue
		}
		peer := Peer{
			NodeID:        node.NodeID,
			Address:       node.Address,
			NodeType:      node.NodeType,
			Capabilities:  map[string]string{},
			LastHeartbeat: time.Now(),
		}
		data, _ := json.Marshal(peer)
		http.Post(BASE_URL+":"+os.Getenv("DISCOVERY_PORT")+"/gossip/register", "application/json", bytes.NewReader(data))
	}
}

func main() {
	var BASE_URL string = os.Getenv("BASE_URL")
	if BASE_URL == "" {
		BASE_URL = "http://localhost"
		log.Println("BASE_URL set to default")
	}
	cfg := config.LoadConfig()
	nodeType := os.Getenv("NODE_TYPE")
	if nodeType != "construction" {
		log.Fatalf("NODE_TYPE must be 'construction'")
	}
	myNodeID = os.Getenv("NODE_ID")
	if myNodeID == "" {
		log.Fatalf("NODE_ID must be set")
	}

	// Initialize tracing and mTLS.
	cleanup := utils.InitTracer("vault-construction")
	defer cleanup()
	/*tlsConfig, err := utils.LoadTLSConfig("/home/tnxl/storage-engine/vault-storage-engine/certs/server.crt",
		"/home/tnxl/storage-engine/vault-storage-engine/certs/server.key",
		"/home/tnxl/storage-engine/vault-storage-engine/certs/ca.crt", true)
	if err != nil {
		log.Fatalf("TLS config error: %v", err)
	} */

	/* mtlsClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	} */

	db, err := database.InitDB()
	if err != nil {
		log.Fatalf("DB init error: %v", err)
	}
	defer db.Close()

	store := sharding.NewLocalShardStore(cfg.ShardStoreBasePath)

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Logger init error: %v", err)
	}

	startDiscoveryAndP2P()

	// We'll give embedded server 1s to bind
	time.Sleep(1 * time.Second)

	// Register with Discovery Service.
	discoveryPort := os.Getenv("DISCOVERY_PORT")
	discoveryURL := fmt.Sprintf("%s:%s", BASE_URL, discoveryPort)
	log.Printf("Discovery URL set to %s", discoveryURL)
	if discoveryURL == "" {
		discoveryURL = "http://localhost:9000"
		log.Fatal("DISCOVERY_URL must be set for storage node")
	}

	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		// Run once on startup
		//registerAllDiscoveredPeers(discoveryURL)

		for {
			select {
			case <-ticker.C:
				registerAllDiscoveredPeersFromRegistry()
			}
		}
	}()

	r := mux.NewRouter()

	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode("This node says Hello, World!")
	})

	r.HandleFunc("/ping", pingPong).Methods("GET")

	r.HandleFunc("/health", handleHealth).Methods("GET")

	r.HandleFunc("/info", handleInfo).Methods("GET")

	r.HandleFunc("/tokens", handleTokenBalance).Methods("GET")

	r.HandleFunc("/shards/info", func(w http.ResponseWriter, r *http.Request) {
		handleShardInfo(w, r, db)
	}).Methods("POST")

	r.HandleFunc("/metrics", handleMetrics).Methods("GET")

	r.HandleFunc("/process", func(w http.ResponseWriter, r *http.Request) {
		handleProcessFile(w, r, db, store, cfg, logger)
	}).Methods("POST")

	r.HandleFunc("/delete", func(w http.ResponseWriter, r *http.Request) {
		handleDeleteFileFromStorage(w, r, db, store, cfg, logger)
	}).Methods("DELETE") // This should delete a file from all storage locations

	r.HandleFunc("/delete/all", func(w http.ResponseWriter, r *http.Request) {
		handleDeleteAllFileVersionsFromStorage(w, r, db, store, cfg, logger)
	}).Methods("DELETE") // This should delete all versions of an object from all storage locations

	r.HandleFunc("/reconstruct", func(w http.ResponseWriter, r *http.Request) {
		handleReconstructFile(w, r, db, store, cfg, logger)
	}).Methods("POST")

	r.HandleFunc("/versions", func(w http.ResponseWriter, r *http.Request) {
		handleListVersions(w, r, db)
	}).Methods("GET")

	r.HandleFunc("/cpu", func(w http.ResponseWriter, r *http.Request) {
		cpuUsage, err := GetCPUUsage()
		if err != nil {
			http.Error(w, "Failed to get CPU usage", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]float64{
			"cpu_usage_percent": cpuUsage,
		})
	}).Methods("GET")

	r.HandleFunc("/memory", func(w http.ResponseWriter, r *http.Request) {
		memoryUsage := GetMemoryUsage()

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]uint64{
			"memory_usage_bytes": memoryUsage,
		})
	}).Methods("GET")

	port := os.Getenv("CONSTRUCTION_PORT")
	if port == "" {
		port = "8080"
		log.Println("CONSTRUCTION_PORT set to default (8080)")
	}

	srv := &http.Server{
		Addr:    ":" + port,
		Handler: r,
		//TLSConfig: tlsConfig,
	}

	logger.Info("Starting Construction Node", zap.String("node_id", myNodeID), zap.String("port", port))
	log.Fatal(srv.ListenAndServe())
}
