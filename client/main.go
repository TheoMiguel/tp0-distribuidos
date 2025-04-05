package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"github.com/spf13/viper"

	"github.com/7574-sistemas-distribuidos/docker-compose-init/client/common"
)

var log = logging.MustGetLogger("log")

// InitConfig Function that uses viper library to parse configuration parameters.
// Viper is configured to read variables from both environment variables and the
// config file ./config.yaml. Environment variables takes precedence over parameters
// defined in the configuration file. If some of the variables cannot be parsed,
// an error is returned
func InitConfig() (*viper.Viper, error) {
	v := viper.New()

	// Configure viper to read env variables with the CLI_ prefix
	v.AutomaticEnv()
	v.SetEnvPrefix("cli")
	// Use a replacer to replace env variables underscores with points. This let us
	// use nested configurations in the config file and at the same time define
	// env variables for the nested configurations
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Add env variables supported
	v.BindEnv("id")
	v.BindEnv("server", "address")
	// v.BindEnv("loop", "period") // Loop period seems less relevant now
	// v.BindEnv("loop", "amount") // Loop amount seems less relevant now
	v.BindEnv("log", "level")
	v.BindEnv("batch", "maxAmount") // Bind batch max amount
	v.BindEnv("bets_file")          // Bind bets file path

	// Try to read configuration from config file. If config file
	// does not exists then ReadInConfig will fail but configuration
	// can be loaded from the environment variables so we shouldn't
	// return an error in that case
	v.SetConfigFile("./config.yaml")
	if err := v.ReadInConfig(); err != nil {
		fmt.Printf("Configuration could not be read from config file. Using env variables instead")
	}

	// Default values
	v.SetDefault("log.level", "INFO")
	v.SetDefault("batch.maxAmount", 100) // Default batch size
	v.SetDefault("loop.period", "100ms")   // Default loop period (if needed between batches)
	v.SetDefault("bets_file", "./agency.csv") // Default bets file path

	// Parse time.Duration variables and return an error if those variables cannot be parsed
	if _, err := time.ParseDuration(v.GetString("loop.period")); err != nil {
		return nil, errors.Wrapf(err, "Could not parse CLI_LOOP_PERIOD env var as time.Duration.")
	}

	return v, nil
}

// InitLogger Receives the log level to be set in go-logging as a string. This method
// parses the string and set the level to the logger. If the level string is not
// valid an error is returned
func InitLogger(logLevel string) error {
	baseBackend := logging.NewLogBackend(os.Stdout, "", 0)
	format := logging.MustStringFormatter(
		`%{time:2006-01-02 15:04:05} %{level:.5s}     %{message}`,
	)
	backendFormatter := logging.NewBackendFormatter(baseBackend, format)

	backendLeveled := logging.AddModuleLevel(backendFormatter)
	logLevelCode, err := logging.LogLevel(logLevel)
	if err != nil {
		return err
	}
	backendLeveled.SetLevel(logLevelCode, "")

	// Set the backends to be used.
	logging.SetBackend(backendLeveled)
	return nil
}

// PrintConfig Print all the configuration parameters of the program.
// For debugging purposes only
func PrintConfig(v *viper.Viper) {
	log.Infof("action: config | result: success | client_id: %s | server_address: %s | loop_period: %v | log_level: %s | batch_maxAmount: %v | bets_file: %s",
		v.GetString("id"),
		v.GetString("server.address"),
		v.GetDuration("loop.period"),
		v.GetString("log.level"),
		v.GetInt("batch.maxAmount"),
		v.GetString("bets_file"), 
	)
}

func main() {
	v, err := InitConfig()
	if err != nil {
		log.Criticalf("%s", err)
		os.Exit(1)
	}

	if err := InitLogger(v.GetString("log.level")); err != nil {
		log.Criticalf("%s", err)
		os.Exit(1)
	}

	// Print program config with debugging purposes
	PrintConfig(v)

	// Get bets file path from config
	betsFilePath := v.GetString("bets_file")

	// Open the bets file
	betsFile, err := os.Open(betsFilePath)
	if err != nil {
		log.Criticalf("action: open_bets_file | result: fail | file: %s | error: %v", betsFilePath, err)
		os.Exit(1)
	}
	defer betsFile.Close() // Ensure the file is closed when main returns

	log.Infof("action: open_bets_file | result: success | file: %s", betsFilePath)

	clientConfig := common.ClientConfig{
		ServerAddress: v.GetString("server.address"),
		ID:            v.GetString("id"),
		LoopPeriod:  v.GetDuration("loop.period"),
		BatchAmount: v.GetInt("batch.maxAmount"),
	}

	// Pass the file handle to the client constructor
	client := common.NewClient(clientConfig, betsFile)
	client.StartClientLoop()
}

/* 
func readBets() ([]common.BetInfo, error) {
	// read bets from agency.csv
	// agency.csv -> Name, Surname, Document, Birthdate, Number
	// return a slice of BetInfo

	// read the file
	file, err := os.Open("./agency.csv")
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bets := []common.BetInfo{}
	// read the file line by line
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// split the line by comma
		fields := strings.Split(line, ",")
		// create a BetInfo
		bet := common.BetInfo{
			Document:  fields[2],
			Number:    fields[4],
			Firstname: fields[0],
			Lastname:  fields[1],
			Birthdate: fields[3],
		}
		// add the bet to the slice
		bets = append(bets, bet)
	}

	return bets, nil

}
*/
