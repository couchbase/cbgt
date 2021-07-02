//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

function LogsCtrl($scope, $http, $routeParams, $log, $sce, $location) {

	$scope.errorMessage = null;
	$scope.logMessages = "";

	$scope.updateLogs = function() {
		$scope.clearErrorMessage();
		$scope.clearLogMessages();
		$http.get('/api/log').then(function(response) {
			var data = response.data;
			for(var i in data.messages) {
				$scope.logMessages += data.messages[i];
			}
			$scope.events = data.events;
		}, function(response) {
			$scope.errorMessage = response.data;
		});
	};

	$scope.clearErrorMessage = function() {
		$scope.errorMessage = null;
	};

	$scope.clearLogMessages = function() {
		$scope.logMessages = "";
	};

	$scope.updateLogs();
}
