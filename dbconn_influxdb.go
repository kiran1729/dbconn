// Copyright (c) 2015 ZeroStack Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// DBConnInflux implements connection to InfluxDB.
// TODO: This does not implement DBConn interface fully because of GetSession().

package dbconn

import (
  "sync"

  "github.com/golang/glog"
  "github.com/influxdb/influxdb-go"
)

// DBConnInflux implements the dbConn interface to connect to InfluxDB.
type DBConnInflux struct {
  server    string
  adminUser string
  adminPass string
  database  string
  isSecure  bool
  dbUser    string
  dbPass    string
  dbConn    *influxdb.Client
  wg        sync.WaitGroup
}

// NewDBConnInflux returns an instance of DBConnInflux after connecting
// to the InfluxDB server and initializing it.
func NewDBConnInflux(server, adminUser, adminPass, database string,
  isSecure bool, dbUser, dbPass string) (*DBConnInflux, error) {

  d := DBConnInflux{server: server, adminUser: adminUser,
    adminPass: adminPass, database: database, isSecure: isSecure,
    dbUser: dbUser, dbPass: dbPass}
  if err := d.resetDB(); err != nil {
    glog.Error("error initializing InfluxDB")
    return &d, err
  }
  // Now connect with dbUser to get connection to use for all Set/Get.
  if err := d.Connect(); err != nil {
    glog.Error("error connecting to InfluxDB")
    // we should return valid reference even if we fail connection so caller
    // can try reconnecting.
    return &d, err
  }
  return &d, nil
}

// Add increments the waitgroup counter
func (d *DBConnInflux) Add(count int) error {
  d.wg.Add(count)
  return nil
}

// Done decrements the waitgroup counter
func (d *DBConnInflux) Done() error {
  d.wg.Done()
  return nil
}

// Connect connects or reconnects to Cassandra cluster using the info supplied
// in NewDBConnInflux.
func (d *DBConnInflux) Connect() error {
  var err error
  // TODO: do we need to use IsUDP config for any reason?
  d.dbConn, err = influxdb.NewClient(&influxdb.ClientConfig{
    Host:     d.server,
    Database: d.database,
    Username: d.dbUser,
    Password: d.dbPass,
    IsSecure: d.isSecure,
  })
  if err != nil {
    glog.Error("error connecting to InfluxDB")
    return err
  }
  return nil
}

// TODO: Move to the setup/install subsystem.

// resetDB removes all DB tables and recreates them. resetDB is performed with
// admin user but regular operations are with dbUser so resetDB will create a
// new connection than the one stored in dbConn.
func (d *DBConnInflux) resetDB() error {
  client, err := influxdb.NewClient(&influxdb.ClientConfig{
    Host:     d.server,
    Username: d.adminUser,
    Password: d.adminPass,
    IsSecure: d.isSecure,
  })
  if err != nil {
    glog.Error("error connecting to InfluxDB")
    return err
  }
  // Try deleting database. Error is ok since it might not exist.
  if err := client.DeleteDatabase(d.database); err != nil {
    glog.Warning("error deleting database: ", d.database, " err: ", err)
  }
  if err := client.CreateDatabase(d.database); err != nil {
    glog.Error("error creating database: ", d.database, " err: ", err)
    return err
  }
  // Error is ok since user might not exist.
  err = client.DeleteDatabaseUser(d.database, d.dbUser)
  if err != nil {
    glog.Warning("error deleting database user: ", err)
  }
  err = client.CreateDatabaseUser(d.database, d.dbUser, d.dbPass)
  if err != nil {
    glog.Error("error creating database user: ", err)
    return err
  }
  // Make dbUser an admin.
  err = client.AlterDatabasePrivilege(d.database, d.dbUser, true)
  if err != nil {
    glog.Error("error altering database privileges: ", err)
    return err
  }
  return nil
}
