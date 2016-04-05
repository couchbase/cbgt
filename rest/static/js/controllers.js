'use strict';

/* Controllers */

function errorMessage(errorMessageFull, code) {
    if (code == 403 && typeof errorMessageFull == "object") {
        var rv = errorMessageFull.message + ": ";
        for (var x in errorMessageFull.permissions) {
            rv += errorMessageFull.permissions[x];
        }
        return rv;
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
