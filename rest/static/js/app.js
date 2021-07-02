//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

'use strict';

// Declare app level module which depends on filters, and services
var cbgtApp = angular.module('cbgtApp', [
  'ngRoute',
  'cbgtApp.filters',
  'cbgtApp.services',
  'cbgtApp.directives',
  'expvar',
  'ui.bootstrap',
  'ui.bootstrap.modal',
  'ui.bootstrap.tabs',
  'ui.tree'
]);

cbgtApp.config(['$routeProvider', '$locationProvider',
 function($routeProvider, $locationProvider) {
  $routeProvider.when('/indexes/',
                      {templateUrl: '/static/partials/index/list.html',
                       controller: 'IndexesCtrl'});
  $routeProvider.when('/indexes/_new',
                      {templateUrl: '/static/partials/index/new.html',
                       controller: 'IndexNewCtrl'});
  $routeProvider.when('/indexes/:indexName',
                      {templateUrl: '/static/partials/index/index.html',
                       controller: 'IndexCtrl'});
  $routeProvider.when('/indexes/:indexName/_edit',
                      {templateUrl: '/static/partials/index/new.html',
                       controller: 'IndexNewCtrl'});
  $routeProvider.when('/indexes/:indexName/_clone',
                      {templateUrl: '/static/partials/index/new.html',
                       controller: 'IndexNewCtrl'});
  $routeProvider.when('/indexes/:indexName/:tabName',
                      {templateUrl: '/static/partials/index/index.html',
                       controller: 'IndexCtrl'});

  $routeProvider.when('/nodes/',
                      {templateUrl: '/static/partials/node/list.html',
                       controller: 'NodeCtrl'});
  $routeProvider.when('/nodes/:nodeUUID',
                      {templateUrl: '/static/partials/node/node.html',
                       controller: 'NodeCtrl'});
  $routeProvider.when('/nodes/:nodeUUID/:tabName',
                      {templateUrl: '/static/partials/node/node.html',
                       controller: 'NodeCtrl'});

  $routeProvider.when('/monitor/',
                      {templateUrl: '/static/partials/monitor.html',
                       controller: 'MonitorCtrl'});

  $routeProvider.when('/manage/',
                      {templateUrl: '/static/partials/manage.html',
                       controller: 'ManageCtrl'});

  $routeProvider.when('/logs/',
                      {templateUrl: '/static/partials/logs.html',
                       controller: 'LogsCtrl'});

  $routeProvider.otherwise({redirectTo: '/indexes'});

  $locationProvider.html5Mode(true);
}]);
