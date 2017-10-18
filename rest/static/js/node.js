//  Copyright (c) 2017 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

function NodeCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.resultCfg = null;
    $scope.resultCfgJSON = null;
    $scope.nodeDefsKnownArrOrderByHostPort = null;

    $scope.nodeUUID = $routeParams.nodeUUID;
    $scope.tab = $routeParams.tabName;
    if($scope.tab === undefined || $scope.tab === "") {
        $scope.tab = "summary";
    }
    $scope.tabPath = '/static/partials/node/tab-' + $scope.tab + '.html';

    $scope.containerPartColor = function(s) {
        var r = 3.14159;
        for (var i = 0; i < s.length; i++) {
            r = r ^ (r * s.charCodeAt(i));
        }
        v = Math.abs(r).toString(16);
        v1 = v.slice(0, 1);
        v2 = v.slice(1, 3);
        return ('#1' + v1 + v2 + v2);
    }

    $scope.cfgGet = function() {
        $scope.resultCfg = null;
        $scope.resultCfgJSON = null;
        $scope.nodeDefsKnownArrOrderByHostPort = [];
        $http.get('/api/cfg').then(function(response) {
            var data = response.data;
            for (var nodeUUID in data.nodeDefsKnown.nodeDefs) {
                var nodeDef = data.nodeDefsKnown.nodeDefs[nodeUUID];
                nodeDef.containerArr = (nodeDef.container || "").split('/');
                $scope.nodeDefsKnownArrOrderByHostPort.push(nodeDef);
            }
            $scope.nodeDefsKnownArrOrderByHostPort.sort(function(a, b) {
                if (a.hostPort < b.hostPort) {
                    return -1;
                }
                if (a.hostPort > b.hostPort) {
                    return 1;
                }
                return 0;
            });
            $scope.resultCfg = data;
            $scope.resultCfgJSON = JSON.stringify(data, undefined, 2);
        }, function(response) {
            var data = response.data;
            $scope.resultCfg = data;
            $scope.resultCfgJSON = JSON.stringify(data, undefined, 2);
        });
    };

    $scope.cfgGet();
}
