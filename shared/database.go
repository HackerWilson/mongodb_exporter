// Copyright 2017 Percona LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shared

import (
    "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"gopkg.in/mgo.v2"
)


const Namespace = "mongodb"

var (
    dbDocumentTotal = prometheus.NewGaugeVec(prometheus.GaugeOpts{
        Namespace: Namespace,
        Name:      "db_document_total",
        Help:      "Count of total documents in collections.",
    }, []string{"database", "collection"})
)

type resultMap map[string]map[string]float64

func (result resultMap) Export(ch chan<- prometheus.Metric) {
    for db, m := range result {
        for col, num := range m {
            dbDocumentTotal.WithLabelValues(db, col).Set(num)
        }
    }
    dbDocumentTotal.Collect(ch)
}

func (result resultMap) Describe(ch chan<- *prometheus.Desc) {
	dbDocumentTotal.Describe(ch)
}

func GetDBCountResult(session *mgo.Session, db string) resultMap {
    collectionNames, err := session.DB(db).CollectionNames()
    if err != nil {
        log.Errorf("Could not get collection names from db: %s!", db)
        return nil
    }

    result := make(resultMap)
    m := make(map[string]float64)
    for _, col := range collectionNames {
        num, err := session.DB(db).C(col).Count()
        if err != nil {
            log.Errorf("Could not count collection: %s from db: %s!", col, db)
            return nil
        }
        m[col] = float64(num)
    }
    result[db] = m

    return result
}
