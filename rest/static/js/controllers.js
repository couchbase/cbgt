//  Copyright 2017-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

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
