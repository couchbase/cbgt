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

function ManageCtrl($scope, $http, $routeParams, $log, $sce, $location) {

    $scope.resultCfg = null;
    $scope.resultCfgJSON = null;
    $scope.resultCfgRefresh = null;
    $scope.resultManagerKick = null;

    $scope.managerKick = function(managerKickMsg) {
        $scope.resultManagerKick = null;
        $http.post('/api/managerKick?msg=' + managerKickMsg).then(function(response) {
            $scope.resultManagerKick = response.data.status;
        }, function(response) {
            $scope.resultManagerKick = response.data;
        });
    };

    $scope.cfgGet = function() {
        $scope.resultCfg = null;
        $scope.resultCfgJSON = null;
        $http.get('/api/cfg').then(function(response) {
            var data = response.data;
            $scope.resultCfg = data;
            $scope.resultCfgJSON = JSON.stringify(data, undefined, 2);
        }, function(response) {
            var data = response.data;
            $scope.resultCfg = data;
            $scope.resultCfgJSON = JSON.stringify(data, undefined, 2);
        });
    };

    $scope.cfgRefresh = function(managerKickMsg) {
        $scope.resultCfgRefresh = null;
        $http.post('/api/cfgRefresh').then(function(response) {
            $scope.resultCfgRefresh = response.data.status;
            $scope.cfgGet()
        }, function(response) {
            $scope.resultCfgRefresh = response.data.status;
        });
    };

    $scope.cfgGet();
}
