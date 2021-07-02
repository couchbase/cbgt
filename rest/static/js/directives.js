//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

'use strict';

/* Directives */

angular.module('cbgtApp.directives', []).
  directive('appVersion', ['version', function(version) {
    return function(scope, elm, attrs) {
      elm.text(version);
    };
  }]).
  directive('nagPrism', ['$compile', function($compile) {
    return {
        restrict: 'A',
        transclude: true,
        scope: {
          source: '@'
        },
        link: function(scope, element, attrs, controller, transclude) {
            scope.$watch('source', function(v) {
              element.find("code").html(v);

              Prism.highlightElement(element.find("code")[0]);
            });

            transclude(function(clone) {
              if (clone.html() !== undefined) {
                element.find("code").html(clone.html());
                $compile(element.contents())(scope.$parent);
              }
            });
        },
        template: "<code></code>"
    };
}]);
