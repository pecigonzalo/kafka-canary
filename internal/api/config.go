package api

type Config struct {
	Host    string `mapstructure:"host"`
	Port    string `mapstructure:"port"`
	Service string `mapstructure:"service"`
}
