package main

import (
	"log"
	"github.com/coreos/etcd/client"
	"github.com/howeyc/fsnotify"
	"golang.org/x/net/context"
	"os"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
	"encoding/hex"
	"fmt"
)

const (
	configPath         = "/etc/heketi/bolt-monitor.yaml"
	etcdRequestTimeout = 5 * time.Second
)

type EtcdClient struct {
	Client  client.Client
	Keys    client.KeysAPI
	Members client.MembersAPI
}
type Config struct {
	Name string
	Conf struct {
		FilePath    string   `yaml:"filePath"`
		EtcdKeyPath string   `yaml:"etcdKeyPath"`
		Endpoints   []string `yaml:"endpoints"`
		TimeOut     time.Duration      `yaml:"timeOut"`
	}
}

func (m *EtcdClient) Connect(endpoints []string) error {
	cli, err := client.New(client.Config{
		Endpoints:               endpoints,
		Transport:               client.DefaultTransport,
		HeaderTimeoutPerRequest: etcdRequestTimeout,
	})
	if err != nil {
		return err
	}
	m.Client = cli
	m.Keys = client.NewKeysAPI(m.Client)
	return nil
}

func (c *Config) configer() (*Config, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil ,err
	}
	err = yaml.Unmarshal(data, c)
	if err != nil {
		return nil ,err
	}
	return c, nil
}

//checking file on fs
func checkIfFileExist() (bool, error) {
	c := Config{}
	config, err := c.configer()
	if err != nil {
		return false, err
	}
	path := config.Conf.FilePath
	_, err = os.Stat(path)
	if err != nil && err == os.ErrNotExist {
		log.Println("File not exist, waiting for the file")
		return false, nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func ifEtcdKeyExist(m *EtcdClient, etdKeyPath string) (bool, error) {
	kapi := m.Keys
	_, err := kapi.Get(context.Background(), etdKeyPath, nil)
	if err != nil && client.IsKeyNotFound(err){
		return false,nil
	} else if err != nil {
		return false, err
	}
	return true, nil
}

func etcdSyncData(endpoints []string, databasePath string, etcdKeyPath string) error {
	m := EtcdClient{}
	err := m.Connect(endpoints)
	kapi := m.Keys
	r, err := ioutil.ReadFile(databasePath)
	if err != nil {
		return err
	}
//	log.Printf("Data: %s", string(r))
	data := hex.EncodeToString(r)
	_, err = kapi.Set(context.TODO(), etcdKeyPath, data, nil)
	if err != nil {
		return err
	}
	return nil
}

func renderDb(endpoints []string,databasePath string, etcdKeyPath string) error {

	m := EtcdClient{}
	err := m.Connect(endpoints)
	if err != nil {
		return err
	}
	kapi := m.Keys
	resp, err := kapi.Get(context.TODO(), etcdKeyPath, nil)
	if err != nil {
		return err
	}
	encoding, err := hex.DecodeString(resp.Node.Value)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(databasePath,encoding,0600)
	if err != nil {
		return err
	}
	fmt.Println("Rendering file running here")
	return nil
}

func monitorDb(endpoints []string,databasePath string, etcdKeyPath string) {
	fmt.Println("Starting monitor db")
	w, err := fsnotify.NewWatcher()
	if err != nil {
		log.Println(err)
	}
	defer w.Close()
	changed := make(chan bool)
	go func() {
		for {
			event := <-w.Event
			if event.IsModify() == true {
				log.Println("File changed")
				err = etcdSyncData(endpoints,databasePath, etcdKeyPath)
				if err != nil {
					log.Printf("Error occured during data syncing: %v", err)
				}
			}
			log.Println("waiting for the change")
		}
	}()
	err = w.Watch(databasePath)
	if err != nil {
		log.Println("Something went wrong")
	}
	<-changed
}

func main() {
	m := EtcdClient{}
	c := Config{}
	conf, err := c.configer()
	if err != nil {
		log.Printf("error getting config values: %v\n", err)
	}
	etcdKeyPath := conf.Conf.EtcdKeyPath
	databasePath := conf.Conf.FilePath
	endpoints := conf.Conf.Endpoints
	to := conf.Conf.TimeOut
	err = m.Connect(endpoints)
	if err != nil {
		log.Printf("Error connecting etcd: %v\n", err)
	}
	for {
		key, err := ifEtcdKeyExist(&m, etcdKeyPath)
		if err != nil {
			log.Printf("Can't check etcd key: %v\n", err)
		}
		file, err := checkIfFileExist()
		if err != nil {
			log.Printf("Can't check file: %v\n", err)
		}
		switch {
		case key == true && file == true:
			log.Println("Starting monitor changes")
			monitorDb(endpoints,databasePath, etcdKeyPath)
		case key != true && file == true:
			log.Println("Sending data to etcd")
			err = etcdSyncData(endpoints,databasePath,etcdKeyPath)
			if err != nil {
				log.Printf("Cant put data into etcd: %v\n",err)
			}
			monitorDb(endpoints,databasePath, etcdKeyPath)
		case key == true && file != true:
			log.Println("Render file")
			err = renderDb(conf.Conf.Endpoints, databasePath, etcdKeyPath)
			if err != nil {
				log.Printf("Cant render database: %v\n", err)
			}
			monitorDb(endpoints,databasePath, etcdKeyPath)
		case key != true && file != true:
			for {
				time.Sleep(to * time.Second)
				temp, err := checkIfFileExist()
				if err != nil && temp != true {
					panic(err)

				} else {
					break
				}
			}
		}
	}
}
