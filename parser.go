// Package beanstalkd YAML parsing utilities.
package beanstalkd

import (
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"
)

// parseJobStats parses job statistics from YAML data.
func parseJobStats(data string) (*JobStats, error) {
	var stats map[string]interface{}
	if err := yaml.Unmarshal([]byte(data), &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	jobStats := &JobStats{}

	// Parse each field
	if v, ok := stats["id"]; ok {
		if id, err := parseUint64(v); err == nil {
			jobStats.ID = id
		}
	}
	if v, ok := stats["tube"]; ok {
		if tube, ok := v.(string); ok {
			jobStats.Tube = tube
		}
	}
	if v, ok := stats["state"]; ok {
		if state, ok := v.(string); ok {
			jobStats.State = state
		}
	}
	if v, ok := stats["priority"]; ok {
		if priority, err := parseUint32(v); err == nil {
			jobStats.Priority = priority
		}
	}
	if v, ok := stats["age"]; ok {
		if age, err := parseUint32(v); err == nil {
			jobStats.Age = age
		}
	}
	if v, ok := stats["delay"]; ok {
		if delay, err := parseUint32(v); err == nil {
			jobStats.Delay = delay
		}
	}
	if v, ok := stats["ttr"]; ok {
		if ttr, err := parseUint32(v); err == nil {
			jobStats.TTR = ttr
		}
	}
	if v, ok := stats["time-left"]; ok {
		if timeLeft, err := parseUint32(v); err == nil {
			jobStats.TimeLeft = timeLeft
		}
	}
	if v, ok := stats["file"]; ok {
		if file, err := parseUint32(v); err == nil {
			jobStats.File = file
		}
	}
	if v, ok := stats["reserves"]; ok {
		if reserves, err := parseUint32(v); err == nil {
			jobStats.Reserves = reserves
		}
	}
	if v, ok := stats["timeouts"]; ok {
		if timeouts, err := parseUint32(v); err == nil {
			jobStats.Timeouts = timeouts
		}
	}
	if v, ok := stats["releases"]; ok {
		if releases, err := parseUint32(v); err == nil {
			jobStats.Releases = releases
		}
	}
	if v, ok := stats["buries"]; ok {
		if buries, err := parseUint32(v); err == nil {
			jobStats.Buries = buries
		}
	}
	if v, ok := stats["kicks"]; ok {
		if kicks, err := parseUint32(v); err == nil {
			jobStats.Kicks = kicks
		}
	}

	return jobStats, nil
}

// parseTubeStats parses tube statistics from YAML data.
func parseTubeStats(data string) (*TubeStats, error) {
	var stats map[string]interface{}
	if err := yaml.Unmarshal([]byte(data), &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	tubeStats := &TubeStats{}

	// Parse each field
	if v, ok := stats["name"]; ok {
		if name, ok := v.(string); ok {
			tubeStats.Name = name
		}
	}
	if v, ok := stats["current-jobs-urgent"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentJobsUrgent = count
		}
	}
	if v, ok := stats["current-jobs-ready"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentJobsReady = count
		}
	}
	if v, ok := stats["current-jobs-reserved"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentJobsReserved = count
		}
	}
	if v, ok := stats["current-jobs-delayed"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentJobsDelayed = count
		}
	}
	if v, ok := stats["current-jobs-buried"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentJobsBuried = count
		}
	}
	if v, ok := stats["total-jobs"]; ok {
		if count, err := parseUint64(v); err == nil {
			tubeStats.TotalJobs = count
		}
	}
	if v, ok := stats["current-using"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentUsing = count
		}
	}
	if v, ok := stats["current-waiting"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentWaiting = count
		}
	}
	if v, ok := stats["current-watching"]; ok {
		if count, err := parseUint32(v); err == nil {
			tubeStats.CurrentWatching = count
		}
	}
	if v, ok := stats["pause"]; ok {
		if pause, err := parseUint32(v); err == nil {
			tubeStats.Pause = pause
		}
	}
	if v, ok := stats["cmd-delete"]; ok {
		if count, err := parseUint64(v); err == nil {
			tubeStats.CmdDelete = count
		}
	}
	if v, ok := stats["cmd-pause-tube"]; ok {
		if count, err := parseUint64(v); err == nil {
			tubeStats.CmdPauseTube = count
		}
	}
	if v, ok := stats["pause-time-left"]; ok {
		if timeLeft, err := parseUint32(v); err == nil {
			tubeStats.PauseTimeLeft = timeLeft
		}
	}

	return tubeStats, nil
}

// parseServerStats parses server statistics from YAML data.
func parseServerStats(data string) (*ServerStats, error) {
	var stats map[string]interface{}
	if err := yaml.Unmarshal([]byte(data), &stats); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}

	serverStats := &ServerStats{}

	// Parse each field
	if v, ok := stats["current-jobs-urgent"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentJobsUrgent = count
		}
	}
	if v, ok := stats["current-jobs-ready"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentJobsReady = count
		}
	}
	if v, ok := stats["current-jobs-reserved"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentJobsReserved = count
		}
	}
	if v, ok := stats["current-jobs-delayed"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentJobsDelayed = count
		}
	}
	if v, ok := stats["current-jobs-buried"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentJobsBuried = count
		}
	}
	if v, ok := stats["cmd-put"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdPut = count
		}
	}
	if v, ok := stats["cmd-peek"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdPeek = count
		}
	}
	if v, ok := stats["cmd-peek-ready"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdPeekReady = count
		}
	}
	if v, ok := stats["cmd-peek-delayed"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdPeekDelayed = count
		}
	}
	if v, ok := stats["cmd-peek-buried"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdPeekBuried = count
		}
	}
	if v, ok := stats["cmd-reserve"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdReserve = count
		}
	}
	if v, ok := stats["cmd-reserve-with-timeout"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdReserveWithTimeout = count
		}
	}
	if v, ok := stats["cmd-touch"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdTouch = count
		}
	}
	if v, ok := stats["cmd-use"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdUse = count
		}
	}
	if v, ok := stats["cmd-watch"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdWatch = count
		}
	}
	if v, ok := stats["cmd-ignore"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdIgnore = count
		}
	}
	if v, ok := stats["cmd-delete"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdDelete = count
		}
	}
	if v, ok := stats["cmd-release"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdRelease = count
		}
	}
	if v, ok := stats["cmd-bury"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdBury = count
		}
	}
	if v, ok := stats["cmd-kick"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdKick = count
		}
	}
	if v, ok := stats["cmd-stats"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdStats = count
		}
	}
	if v, ok := stats["cmd-stats-job"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdStatsJob = count
		}
	}
	if v, ok := stats["cmd-stats-tube"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdStatsTube = count
		}
	}
	if v, ok := stats["cmd-list-tubes"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdListTubes = count
		}
	}
	if v, ok := stats["cmd-list-tube-used"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdListTubeUsed = count
		}
	}
	if v, ok := stats["cmd-list-tubes-watched"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdListTubesWatched = count
		}
	}
	if v, ok := stats["cmd-pause-tube"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.CmdPauseTube = count
		}
	}
	if v, ok := stats["job-timeouts"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.JobTimeouts = count
		}
	}
	if v, ok := stats["total-jobs"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.TotalJobs = count
		}
	}
	if v, ok := stats["max-job-size"]; ok {
		if size, err := parseUint32(v); err == nil {
			serverStats.MaxJobSize = size
		}
	}
	if v, ok := stats["current-tubes"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentTubes = count
		}
	}
	if v, ok := stats["current-connections"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentConnections = count
		}
	}
	if v, ok := stats["current-producers"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentProducers = count
		}
	}
	if v, ok := stats["current-workers"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentWorkers = count
		}
	}
	if v, ok := stats["current-waiting"]; ok {
		if count, err := parseUint32(v); err == nil {
			serverStats.CurrentWaiting = count
		}
	}
	if v, ok := stats["total-connections"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.TotalConnections = count
		}
	}
	if v, ok := stats["pid"]; ok {
		if pid, err := parseUint32(v); err == nil {
			serverStats.PID = pid
		}
	}
	if v, ok := stats["version"]; ok {
		if version, ok := v.(string); ok {
			serverStats.Version = version
		}
	}
	if v, ok := stats["rusage-utime"]; ok {
		if utime, ok := v.(string); ok {
			serverStats.RusageUtime = utime
		}
	}
	if v, ok := stats["rusage-stime"]; ok {
		if stime, ok := v.(string); ok {
			serverStats.RusageStime = stime
		}
	}
	if v, ok := stats["uptime"]; ok {
		if uptime, err := parseUint64(v); err == nil {
			serverStats.Uptime = uptime
		}
	}
	if v, ok := stats["binlog-oldest-index"]; ok {
		if index, err := parseUint32(v); err == nil {
			serverStats.BinlogOldestIndex = index
		}
	}
	if v, ok := stats["binlog-current-index"]; ok {
		if index, err := parseUint32(v); err == nil {
			serverStats.BinlogCurrentIndex = index
		}
	}
	if v, ok := stats["binlog-max-size"]; ok {
		if size, err := parseUint32(v); err == nil {
			serverStats.BinlogMaxSize = size
		}
	}
	if v, ok := stats["binlog-records-written"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.BinlogRecordsWritten = count
		}
	}
	if v, ok := stats["binlog-records-migrated"]; ok {
		if count, err := parseUint64(v); err == nil {
			serverStats.BinlogRecordsMigrated = count
		}
	}
	if v, ok := stats["draining"]; ok {
		if draining, ok := v.(bool); ok {
			serverStats.Draining = draining
		}
	}
	if v, ok := stats["id"]; ok {
		if id, ok := v.(string); ok {
			serverStats.ID = id
		}
	}
	if v, ok := stats["hostname"]; ok {
		if hostname, ok := v.(string); ok {
			serverStats.Hostname = hostname
		}
	}
	if v, ok := stats["os"]; ok {
		if os, ok := v.(string); ok {
			serverStats.OS = os
		}
	}
	if v, ok := stats["platform"]; ok {
		if platform, ok := v.(string); ok {
			serverStats.Platform = platform
		}
	}

	return serverStats, nil
}

// parseTubeList parses a list of tube names from YAML data.
func parseTubeList(data string) ([]string, error) {
	var tubes []string
	if err := yaml.Unmarshal([]byte(data), &tubes); err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
	}
	return tubes, nil
}

// parseUint32 parses a value to uint32.
func parseUint32(v interface{}) (uint32, error) {
	switch val := v.(type) {
	case int:
		return uint32(val), nil
	case int32:
		return uint32(val), nil
	case int64:
		return uint32(val), nil
	case uint:
		return uint32(val), nil
	case uint32:
		return val, nil
	case uint64:
		return uint32(val), nil
	case float64:
		return uint32(val), nil
	case string:
		parsed, err := strconv.ParseUint(val, 10, 32)
		if err != nil {
			return 0, err
		}
		return uint32(parsed), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint32", v)
	}
}

// parseUint64 parses a value to uint64.
func parseUint64(v interface{}) (uint64, error) {
	switch val := v.(type) {
	case int:
		return uint64(val), nil
	case int32:
		return uint64(val), nil
	case int64:
		return uint64(val), nil
	case uint:
		return uint64(val), nil
	case uint32:
		return uint64(val), nil
	case uint64:
		return val, nil
	case float64:
		return uint64(val), nil
	case string:
		parsed, err := strconv.ParseUint(val, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to uint64", v)
	}
}
