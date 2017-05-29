/**
 * Deletes the document if exists
 * @param {string} id - the document id
 * @returns {boolean} true if deleted; else false 
 */
function deleteDocumentIfExists(id) {
    var context = getContext();
    var collection = context.getCollection();
    var response = context.getResponse();

    var result = collection.filter(function (doc) { return doc.id === id; }, function (err, documents) {
        if (err) throw err;
        if (documents.length > 1) throw new ("Found more than one document for id :" + id);
        response.setBody(false);

        if (documents.length === 1) {
            var self = documents[0]._self;
            collection.deleteDocument(self);
            response.setBody(true);
        }
    });

    if (!result.isAccepted) throw new ("The call was not accepted");
}