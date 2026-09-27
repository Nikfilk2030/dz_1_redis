package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"gamehub/internal/config"
	"gamehub/internal/redisconn"
	"github.com/redis/go-redis/v9"
)

const loginScript = `
local count = redis.call('INCR', KEYS[1])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1]))
return {count, redis.call('TTL', KEYS[1])}
`

type server struct {
	master  *redis.Client
	replica *redis.Client
}

type apiError struct {
	status int
	detail string
}

func (e *apiError) Error() string { return e.detail }

type endpoint func(http.ResponseWriter, *http.Request) error

// Указатели помогают отличить пропущенное поле от нуля в JSON
type playerInput struct {
	Name      *string `json:"name"`
	Level     *int64  `json:"level"`
	Region    *string `json:"region"`
	CreatedAt *string `json:"created_at"`
}

type batchPlayerInput struct {
	ID        *int64  `json:"id"`
	Name      *string `json:"name"`
	Level     *int64  `json:"level"`
	Region    *string `json:"region"`
	CreatedAt *string `json:"created_at"`
}

type playerOutput struct {
	ID        int64  `json:"id"`
	Name      string `json:"name"`
	Level     int64  `json:"level"`
	Region    string `json:"region"`
	CreatedAt string `json:"created_at"`
}

func main() {
	s := &server{master: redisconn.Master(), replica: redisconn.Replica()}
	defer s.master.Close()
	defer s.replica.Close()
	if err := s.waitForRedis(); err != nil {
		log.Fatal(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handle(s.health))
	mux.HandleFunc("POST /api/players/batch", s.handle(s.createPlayersBatch))
	mux.HandleFunc("POST /api/players/{player_id}", s.handle(s.createPlayer))
	mux.HandleFunc("GET /api/players/{player_id}", s.handle(s.getPlayer))
	mux.HandleFunc("PATCH /api/players/{player_id}/level", s.handle(s.changeLevel))
	mux.HandleFunc("POST /api/players/{player_id}/login", s.handle(s.recordLogin))
	mux.HandleFunc("POST /api/leaderboard/score", s.handle(s.addScore))
	mux.HandleFunc("GET /api/leaderboard/top", s.handle(s.leaderboardTop))
	mux.HandleFunc("GET /api/leaderboard/rank/{player_id}", s.handle(s.leaderboardRank))
	mux.HandleFunc("POST /api/players/{player_id}/achievements", s.handle(s.addAchievement))
	mux.HandleFunc("GET /api/players/{player_id}/achievements/{name}", s.handle(s.hasAchievement))
	mux.HandleFunc("GET /api/players/{id1}/achievements/common/{id2}", s.handle(s.commonAchievements))

	log.Printf("API listening on %s", config.APIListenAddress)
	log.Fatal(http.ListenAndServe(config.APIListenAddress, mux))
}

func (s *server) waitForRedis() error {
	// Sentinel и Redis стартуют не одновременно, даём им время договориться о мастере
	deadline := time.Now().Add(config.RedisStartupTimeout)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), config.RedisStartupAttemptTimeout)
		err := s.master.Ping(ctx).Err()
		if err == nil {
			err = s.master.XGroupCreateMkStream(ctx, config.NotificationsStream, config.NotificationsGroup, config.StreamGroupNewOnlyID).Err()
			if err == nil || strings.HasPrefix(err.Error(), config.RedisBusyGroupPrefix) {
				cancel()
				return nil
			}
		}
		cancel()
		time.Sleep(config.RedisStartupRetryDelay)
	}
	return errors.New("Redis or notifications group did not become ready")
}

func (s *server) handle(next endpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := next(w, r); err != nil {
			var badRequest *apiError
			if errors.As(err, &badRequest) {
				writeJSON(w, badRequest.status, map[string]string{"detail": badRequest.detail})
				return
			}
			log.Printf("request failed: %v", err)
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"detail": "Redis is temporarily unavailable"})
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

func readJSON(w http.ResponseWriter, r *http.Request, value any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, config.MaxRequestBodyBytes))
	if err := decoder.Decode(value); err != nil {
		return &apiError{http.StatusUnprocessableEntity, "Invalid request body"}
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return &apiError{http.StatusUnprocessableEntity, "Invalid request body"}
	}
	return nil
}

func playerID(r *http.Request, name string) (int64, error) {
	id, err := strconv.ParseInt(r.PathValue(name), 10, 64)
	if err != nil || id < 1 {
		return 0, &apiError{http.StatusUnprocessableEntity, "Player ID must be positive"}
	}
	return id, nil
}

func playerKey(id int64) string      { return fmt.Sprintf("player:%d", id) }
func achievementKey(id int64) string { return fmt.Sprintf("achievements:%d", id) }
func cacheKey(id int64) string       { return fmt.Sprintf("cache:player:%d", id) }

func validatePlayer(player playerInput) error {
	if player.Name == nil || utf8.RuneCountInString(*player.Name) < 1 || utf8.RuneCountInString(*player.Name) > config.MaxPlayerNameRunes ||
		player.Level == nil || *player.Level < 1 ||
		player.Region == nil || utf8.RuneCountInString(*player.Region) < 1 || utf8.RuneCountInString(*player.Region) > config.MaxRegionRunes {
		return &apiError{http.StatusUnprocessableEntity, "Invalid player data"}
	}
	return nil
}

func timestamp(value *string) (string, error) {
	if value == nil {
		return time.Now().UTC().Format(config.TimestampLayout), nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, *value)
	if err != nil {
		for _, plain := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05"} {
			parsed, err = time.ParseInLocation(plain, *value, time.UTC)
			if err == nil {
				break
			}
		}
	}
	if err != nil {
		return "", &apiError{http.StatusUnprocessableEntity, "Invalid created_at"}
	}
	return parsed.UTC().Format(config.TimestampLayout), nil
}

func playerData(player playerInput) (map[string]string, error) {
	if err := validatePlayer(player); err != nil {
		return nil, err
	}
	createdAt, err := timestamp(player.CreatedAt)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"name":       *player.Name,
		"level":      strconv.FormatInt(*player.Level, 10),
		"region":     *player.Region,
		"created_at": createdAt,
	}, nil
}

func playerResponse(id int64, raw map[string]string) (playerOutput, error) {
	level, err := strconv.ParseInt(raw["level"], 10, 64)
	if err != nil {
		return playerOutput{}, err
	}
	return playerOutput{id, raw["name"], level, raw["region"], raw["created_at"]}, nil
}

func (s *server) requirePlayer(ctx context.Context, id int64) error {
	exists, err := s.master.Exists(ctx, playerKey(id)).Result()
	if err != nil {
		return err
	}
	if exists == 0 {
		return &apiError{http.StatusNotFound, "Player not found"}
	}
	return nil
}

func (s *server) health(w http.ResponseWriter, r *http.Request) error {
	if err := s.master.Ping(r.Context()).Err(); err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	return nil
}

func (s *server) createPlayersBatch(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		Players []batchPlayerInput `json:"players"`
	}
	if err := readJSON(w, r, &body); err != nil {
		return err
	}
	if len(body.Players) < 1 || len(body.Players) > config.MaxBatchPlayers {
		return &apiError{http.StatusUnprocessableEntity, "Batch must contain 1 to 100 players"}
	}

	ids := make([]int64, 0, len(body.Players))
	data := make([]map[string]string, 0, len(body.Players))
	seen := make(map[int64]bool)
	for _, player := range body.Players {
		if player.ID == nil || *player.ID < 1 {
			return &apiError{http.StatusUnprocessableEntity, "Player ID must be positive"}
		}
		if seen[*player.ID] {
			return &apiError{http.StatusUnprocessableEntity, "Player IDs must be unique"}
		}
		seen[*player.ID] = true
		profile, err := playerData(playerInput{player.Name, player.Level, player.Region, player.CreatedAt})
		if err != nil {
			return err
		}
		ids = append(ids, *player.ID)
		data = append(data, profile)
	}

	started := time.Now()
	ctx := r.Context()
	// Сначала читаем старые даты одним пакетом, потом обновляем все профили одной транзакцией
	readPipe := s.master.Pipeline()
	previous := make(map[int]*redis.StringCmd)
	for i, player := range body.Players {
		if player.CreatedAt == nil {
			previous[i] = readPipe.HGet(ctx, playerKey(ids[i]), "created_at")
		}
	}
	if len(previous) > 0 {
		if _, err := readPipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
			return err
		}
		for i, cmd := range previous {
			if old, err := cmd.Result(); err == nil {
				data[i]["created_at"] = old
			} else if !errors.Is(err, redis.Nil) {
				return err
			}
		}
	}

	pipe := s.master.TxPipeline()
	for i, id := range ids {
		pipe.HSet(ctx, playerKey(id), data[i])
		pipe.Del(ctx, cacheKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	elapsedMS := math.Round(time.Since(started).Seconds()*1_000_000) / 1000
	writeJSON(w, http.StatusOK, map[string]any{"loaded": len(ids), "player_ids": ids, "elapsed_ms": elapsedMS})
	return nil
}

func (s *server) createPlayer(w http.ResponseWriter, r *http.Request) error {
	id, err := playerID(r, "player_id")
	if err != nil {
		return err
	}
	var player playerInput
	if err := readJSON(w, r, &player); err != nil {
		return err
	}
	data, err := playerData(player)
	if err != nil {
		return err
	}
	ctx := r.Context()
	existed, err := s.master.Exists(ctx, playerKey(id)).Result()
	if err != nil {
		return err
	}
	if existed > 0 && player.CreatedAt == nil {
		old, err := s.master.HGet(ctx, playerKey(id), "created_at").Result()
		if err == nil {
			data["created_at"] = old
		} else if !errors.Is(err, redis.Nil) {
			return err
		}
	}
	pipe := s.master.TxPipeline()
	pipe.HSet(ctx, playerKey(id), data)
	pipe.Del(ctx, cacheKey(id))
	if _, err := pipe.Exec(ctx); err != nil {
		return err
	}
	profile, err := playerResponse(id, data)
	if err != nil {
		return err
	}
	status := http.StatusCreated
	if existed > 0 {
		status = http.StatusOK
	}
	writeJSON(w, status, profile)
	return nil
}

func (s *server) getPlayer(w http.ResponseWriter, r *http.Request) error {
	id, err := playerID(r, "player_id")
	if err != nil {
		return err
	}
	ctx := r.Context()
	cached, err := s.master.Get(ctx, cacheKey(id)).Result()
	if err == nil {
		w.Header().Set("X-Cache", "HIT")
		writeJSON(w, http.StatusOK, json.RawMessage(cached))
		return nil
	}
	if !errors.Is(err, redis.Nil) {
		return err
	}
	raw, err := s.master.HGetAll(ctx, playerKey(id)).Result()
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return &apiError{http.StatusNotFound, "Player not found"}
	}
	profile, err := playerResponse(id, raw)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	_ = s.master.Set(ctx, cacheKey(id), encoded, config.ProfileCacheTTL).Err()
	w.Header().Set("X-Cache", "MISS")
	writeJSON(w, http.StatusOK, profile)
	return nil
}

func (s *server) changeLevel(w http.ResponseWriter, r *http.Request) error {
	id, err := playerID(r, "player_id")
	if err != nil {
		return err
	}
	var body struct {
		Delta *int64 `json:"delta"`
	}
	if err := readJSON(w, r, &body); err != nil {
		return err
	}
	if body.Delta == nil || *body.Delta == 0 {
		return &apiError{http.StatusUnprocessableEntity, "Delta must not be zero"}
	}

	ctx := r.Context()
	var level int64
	var notificationID string
	for {
		err = s.master.Watch(ctx, func(tx *redis.Tx) error {
			current, err := tx.HGet(ctx, playerKey(id), "level").Int64()
			if errors.Is(err, redis.Nil) {
				return &apiError{http.StatusNotFound, "Player not found"}
			}
			if err != nil {
				return err
			}
			if current+*body.Delta < 1 {
				return &apiError{http.StatusUnprocessableEntity, "Level must remain positive"}
			}
			now := time.Now().UTC()
			cutoff := fmt.Sprintf("%d-0", now.Add(-config.NotificationsRetention).UnixMilli())
			var levelCmd *redis.IntCmd
			var notificationCmd *redis.StringCmd
			// WATCH защищает проверку уровня от двух одновременных PATCH-запросов
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				levelCmd = pipe.HIncrBy(ctx, playerKey(id), "level", *body.Delta)
				pipe.Del(ctx, cacheKey(id))
				notificationCmd = pipe.XAdd(ctx, &redis.XAddArgs{
					Stream: config.NotificationsStream,
					Values: map[string]any{
						"player_id": id,
						"type":      config.LevelChangedEventType,
						"message":   fmt.Sprintf("Player %d level changed by %+d", id, *body.Delta),
						"timestamp": now.Format(config.TimestampLayout),
					},
				})
				pipe.XTrimMinID(ctx, config.NotificationsStream, cutoff)
				return nil
			})
			if err == nil {
				level = levelCmd.Val()
				notificationID = notificationCmd.Val()
			}
			return err
		}, playerKey(id))
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		if err != nil {
			return err
		}
		break
	}
	writeJSON(w, http.StatusOK, map[string]any{"player_id": id, "level": level, "notification_id": notificationID})
	return nil
}

func (s *server) recordLogin(w http.ResponseWriter, r *http.Request) error {
	id, err := playerID(r, "player_id")
	if err != nil {
		return err
	}
	ctx := r.Context()
	if err := s.requirePlayer(ctx, id); err != nil {
		return err
	}
	// INCR и обновление TTL нужны как одна операция, поэтому здесь короткий Lua-скрипт
	result, err := s.master.Eval(ctx, loginScript, []string{fmt.Sprintf("logins:%d", id)}, int64(config.LoginTTL/time.Second)).Result()
	if err != nil {
		return err
	}
	values, ok := result.([]any)
	if !ok || len(values) != 2 {
		return errors.New("unexpected login script result")
	}
	count, err := asInt64(values[0])
	if err != nil {
		return err
	}
	ttl, err := asInt64(values[1])
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"player_id": id, "login_count": count, "ttl_seconds": ttl})
	return nil
}

func asInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unexpected Redis integer: %T", value)
	}
}

func (s *server) addScore(w http.ResponseWriter, r *http.Request) error {
	var body struct {
		PlayerID *int64 `json:"player_id"`
		Score    *int64 `json:"score"`
	}
	if err := readJSON(w, r, &body); err != nil {
		return err
	}
	if body.PlayerID == nil || *body.PlayerID < 1 || body.Score == nil {
		return &apiError{http.StatusUnprocessableEntity, "Invalid score data"}
	}
	ctx := r.Context()
	if err := s.requirePlayer(ctx, *body.PlayerID); err != nil {
		return err
	}
	score, err := s.master.ZIncrBy(ctx, config.LeaderboardKey, float64(*body.Score), strconv.FormatInt(*body.PlayerID, 10)).Result()
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"player_id": *body.PlayerID, "score": score})
	return nil
}

func (s *server) leaderboardTop(w http.ResponseWriter, r *http.Request) error {
	limit := config.DefaultLeaderboardLimit
	if values, ok := r.URL.Query()["limit"]; ok {
		value := values[0]
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil || parsed < 1 || parsed > config.MaxLeaderboardLimit {
			return &apiError{http.StatusUnprocessableEntity, "Limit must be between 1 and 100"}
		}
		limit = parsed
	}
	ctx := r.Context()
	entries, err := s.replica.ZRevRangeWithScores(ctx, config.LeaderboardKey, 0, limit-1).Result()
	// Реплика иногда ещё не получила свежую запись, поэтому пустой ответ перепроверяем у мастера
	if err != nil || len(entries) == 0 {
		entries, err = s.master.ZRevRangeWithScores(ctx, config.LeaderboardKey, 0, limit-1).Result()
		if err != nil {
			return err
		}
	}
	players := make([]map[string]any, 0, len(entries))
	for position, item := range entries {
		id, err := strconv.ParseInt(fmt.Sprint(item.Member), 10, 64)
		if err != nil {
			return err
		}
		players = append(players, map[string]any{"rank": position + 1, "player_id": id, "score": item.Score})
	}
	writeJSON(w, http.StatusOK, map[string]any{"players": players})
	return nil
}

func (s *server) leaderboardRank(w http.ResponseWriter, r *http.Request) error {
	id, err := playerID(r, "player_id")
	if err != nil {
		return err
	}
	ctx := r.Context()
	member := strconv.FormatInt(id, 10)
	rank, err := s.replica.ZRevRank(ctx, config.LeaderboardKey, member).Result()
	readFrom := s.replica
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			log.Printf("replica rank read failed: %v", err)
		}
		rank, err = s.master.ZRevRank(ctx, config.LeaderboardKey, member).Result()
		readFrom = s.master
	}
	if errors.Is(err, redis.Nil) {
		return &apiError{http.StatusNotFound, "Player is not on the leaderboard"}
	}
	if err != nil {
		return err
	}
	score, err := readFrom.ZScore(ctx, config.LeaderboardKey, member).Result()
	if err != nil && readFrom == s.replica {
		score, err = s.master.ZScore(ctx, config.LeaderboardKey, member).Result()
	}
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"player_id": id, "rank": rank + 1, "score": score})
	return nil
}

func (s *server) addAchievement(w http.ResponseWriter, r *http.Request) error {
	id, err := playerID(r, "player_id")
	if err != nil {
		return err
	}
	var body struct {
		Name *string `json:"achievement_name"`
	}
	if err := readJSON(w, r, &body); err != nil {
		return err
	}
	if body.Name == nil || utf8.RuneCountInString(*body.Name) < 1 || utf8.RuneCountInString(*body.Name) > config.MaxAchievementNameRunes {
		return &apiError{http.StatusUnprocessableEntity, "Invalid achievement name"}
	}
	ctx := r.Context()
	if err := s.requirePlayer(ctx, id); err != nil {
		return err
	}
	added, err := s.master.SAdd(ctx, achievementKey(id), *body.Name).Result()
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"player_id": id, "achievement_name": *body.Name, "added": added > 0})
	return nil
}

func (s *server) hasAchievement(w http.ResponseWriter, r *http.Request) error {
	id, err := playerID(r, "player_id")
	if err != nil {
		return err
	}
	ctx := r.Context()
	if err := s.requirePlayer(ctx, id); err != nil {
		return err
	}
	name := r.PathValue("name")
	exists, err := s.replica.SIsMember(ctx, achievementKey(id), name).Result()
	if err != nil || !exists {
		exists, err = s.master.SIsMember(ctx, achievementKey(id), name).Result()
	}
	if err != nil {
		return err
	}
	writeJSON(w, http.StatusOK, map[string]any{"player_id": id, "achievement_name": name, "has_achievement": exists})
	return nil
}

func (s *server) commonAchievements(w http.ResponseWriter, r *http.Request) error {
	id1, err := playerID(r, "id1")
	if err != nil {
		return err
	}
	id2, err := playerID(r, "id2")
	if err != nil {
		return err
	}
	ctx := r.Context()
	if err := s.requirePlayer(ctx, id1); err != nil {
		return err
	}
	if err := s.requirePlayer(ctx, id2); err != nil {
		return err
	}
	common, err := s.replica.SInter(ctx, achievementKey(id1), achievementKey(id2)).Result()
	if err != nil || len(common) == 0 {
		common, err = s.master.SInter(ctx, achievementKey(id1), achievementKey(id2)).Result()
	}
	if err != nil {
		return err
	}
	sort.Strings(common)
	if common == nil {
		common = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"player_ids": []int64{id1, id2}, "achievements": common})
	return nil
}
