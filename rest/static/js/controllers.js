'use strict';

/* Controllers */

function errorMessage(errorMessageFull, code) {
    console.log("errorMessageFull", errorMessageFull, code);
    var a = (errorMessageFull || (code + "")).split("err: ");
    return a[a.length - 1];
}

cbgtApp.controller({
    'IndexesCtrl': IndexesCtrl,
    'IndexNewCtrl': IndexNewCtrl,
    'IndexCtrl': IndexCtrl,
    'NodeCtrl': NodeCtrl,
    'MonitorCtrl': MonitorCtrl,
    'ManageCtrl': ManageCtrl,
    'LogsCtrl': LogsCtrl
});
