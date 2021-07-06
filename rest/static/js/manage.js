//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
