sap.ui.define([
    "sap/ui/core/mvc/ControllerExtension",
    "sap/ui/model/json/JSONModel",
    "sap/m/MessageBox",
    "sap/m/MessageToast"
], function (ControllerExtension, JSONModel, MessageBox, MessageToast) {
    "use strict";

    return ControllerExtension.extend("cds.plugin.caching.dashboardfe.ext.ObjectPageExtension", {

        override: {
            onInit: function () {
                this.base.getView().setModel(new JSONModel({ entries: [] }), "entriesModel");
            }
        },

        onLoadCacheEntries: function () {
            const oContext   = this.base.getView().getBindingContext();
            const oModel     = this.base.getView().getModel();
            const oFnBinding = oModel.bindContext("plugin.cds_caching.CachingApiService.getEntries(...)", oContext, { $$inheritExpandSelect: false });
            oFnBinding.setParameter("top",  100);
            oFnBinding.setParameter("skip", 0);
            oFnBinding.execute().then(() => {
                const oResult  = oFnBinding.getBoundContext().getObject();
                const aEntries = Array.isArray(oResult) ? oResult : (oResult ? [oResult] : []);
                this.base.getView().getModel("entriesModel").setProperty("/entries", aEntries);
            }).catch((oErr) => {
                MessageBox.error("Failed to load entries: " + (oErr.message || String(oErr)));
            });
        },

        onGetEntry: function () {
            const oView      = this.base.getView();
            const oContext   = oView.getBindingContext();
            const oModel     = oView.getModel();
            const sKey       = oView.byId("getKey").getValue();
            if (!sKey) { MessageToast.show("Please enter a key"); return; }

            const oFnBinding = oModel.bindContext("plugin.cds_caching.CachingApiService.getEntry(...)", oContext, { $$inheritExpandSelect: false });
            oFnBinding.setParameter("key", sKey);
            oFnBinding.execute().then(() => {
                const oResult = oFnBinding.getBoundContext().getObject();
                oView.byId("getResult").setValue(oResult ? JSON.stringify(oResult, null, 2) : "Not found");
            }).catch((oErr) => {
                MessageBox.error("Failed to get entry: " + (oErr.message || String(oErr)));
            });
        },

        onSetEntry: function () {
            const oView    = this.base.getView();
            const oContext = oView.getBindingContext();
            const oModel   = oView.getModel();
            const sKey     = oView.byId("createKey").getValue();
            const sValue   = oView.byId("createValue").getValue();
            const iTtl     = parseInt(oView.byId("createTtl").getValue(), 10) || undefined;
            if (!sKey || !sValue) { MessageToast.show("Key and value are required"); return; }

            const oAction = oModel.bindContext("plugin.cds_caching.CachingApiService.setEntry(...)", oContext);
            oAction.setParameter("key",   sKey);
            oAction.setParameter("value", sValue);
            if (iTtl) oAction.setParameter("ttl", iTtl);
            oAction.execute().then(() => {
                MessageToast.show("Entry set successfully");
                oView.byId("createKey").setValue("");
                oView.byId("createValue").setValue("");
                oView.byId("createTtl").setValue("");
            }).catch((oErr) => {
                MessageBox.error("Failed to set entry: " + (oErr.message || String(oErr)));
            });
        },

        onDeleteEntry: function () {
            const oView    = this.base.getView();
            const oContext = oView.getBindingContext();
            const oModel   = oView.getModel();
            const sKey     = oView.byId("deleteKey").getValue();
            if (!sKey) { MessageToast.show("Please enter a key"); return; }

            const oAction = oModel.bindContext("plugin.cds_caching.CachingApiService.deleteEntry(...)", oContext);
            oAction.setParameter("key", sKey);
            oAction.execute().then(() => {
                MessageToast.show("Entry deleted");
                oView.byId("deleteKey").setValue("");
            }).catch((oErr) => {
                MessageBox.error("Failed to delete entry: " + (oErr.message || String(oErr)));
            });
        }
    });
});
