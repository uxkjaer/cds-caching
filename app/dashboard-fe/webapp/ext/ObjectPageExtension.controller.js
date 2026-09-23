sap.ui.define([
    "sap/ui/model/json/JSONModel",
    "sap/m/MessageBox",
    "sap/m/MessageToast"
], function (JSONModel, MessageBox, MessageToast) {
    "use strict";

    return {

        onInit: function () {
            this.getView().setModel(new JSONModel({ entries: [] }), "entriesModel");
        },

        onLoadCacheEntries: function () {
            const oContext = this.getView().getBindingContext();
            const oModel   = this.getView().getModel();
            const oFnBinding = oModel.bindContext("getEntries(...)", oContext, { $$inheritExpandSelect: false });
            oFnBinding.setParameter("top",  100);
            oFnBinding.setParameter("skip", 0);
            oFnBinding.execute().then(() => {
                const oResult = oFnBinding.getBoundContext().getObject();
                const aEntries = Array.isArray(oResult) ? oResult : (oResult ? [oResult] : []);
                this.getView().getModel("entriesModel").setProperty("/entries", aEntries);
            }).catch((oErr) => {
                MessageBox.error("Failed to load entries: " + (oErr.message || String(oErr)));
            });
        },

        onGetEntry: function () {
            const oContext  = this.getView().getBindingContext();
            const oModel    = this.getView().getModel();
            const oView     = this.getView();
            const sKey      = oView.byId("getKey").getValue();
            if (!sKey) { MessageToast.show("Please enter a key"); return; }

            const oFnBinding = oModel.bindContext("getEntry(...)", oContext, { $$inheritExpandSelect: false });
            oFnBinding.setParameter("key", sKey);
            oFnBinding.execute().then(() => {
                const oResult = oFnBinding.getBoundContext().getObject();
                oView.byId("getResult").setValue(oResult ? JSON.stringify(oResult, null, 2) : "Not found");
            }).catch((oErr) => {
                MessageBox.error("Failed to get entry: " + (oErr.message || String(oErr)));
            });
        },

        onSetEntry: function () {
            const oContext = this.getView().getBindingContext();
            const oModel   = this.getView().getModel();
            const oView    = this.getView();
            const sKey     = oView.byId("createKey").getValue();
            const sValue   = oView.byId("createValue").getValue();
            const iTtl     = parseInt(oView.byId("createTtl").getValue(), 10) || undefined;
            if (!sKey || !sValue) { MessageToast.show("Key and value are required"); return; }

            const oAction = oModel.bindContext("setEntry(...)", oContext);
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
            const oContext = this.getView().getBindingContext();
            const oModel   = this.getView().getModel();
            const oView    = this.getView();
            const sKey     = oView.byId("deleteKey").getValue();
            if (!sKey) { MessageToast.show("Please enter a key"); return; }

            const oAction = oModel.bindContext("deleteEntry(...)", oContext);
            oAction.setParameter("key", sKey);
            oAction.execute().then(() => {
                MessageToast.show("Entry deleted");
                oView.byId("deleteKey").setValue("");
            }).catch((oErr) => {
                MessageBox.error("Failed to delete entry: " + (oErr.message || String(oErr)));
            });
        }
    };
});
