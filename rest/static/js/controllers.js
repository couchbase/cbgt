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

'use strict';

/* Controllers */

function errorMessage(errorMessageFull, code) {
    if (typeof errorMessageFull == "object") {
        if (code == 403) {
            var rv = errorMessageFull.message + ": ";
            for (var x in errorMessageFull.permissions) {
                rv += errorMessageFull.permissions[x];
            }
            return rv;
        } else {
            errorMessageFull = errorMessageFull.error
        }
    }
    console.log("errorMessageFull", errorMessageFull, code);
    var a = (errorMessageFull || (code + "")).split("err: ");
    return a[a.length - 1];
}

cbgtApp.controller({
    'IndexesCtrl': IndexesCtrl,
    'IndexNewCtrl': IndexNewCtrl,
    'IndexCtrl': IndexCtrl,
    'QueryCtrl': QueryCtrl,
    'NodeCtrl': NodeCtrl,
    'MonitorCtrl': MonitorCtrl,
    'ManageCtrl': ManageCtrl,
    'LogsCtrl': LogsCtrl
});
