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
