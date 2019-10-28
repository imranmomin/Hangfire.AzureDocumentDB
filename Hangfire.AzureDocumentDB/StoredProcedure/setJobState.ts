/**
 * Set the Job state data
 * @param {string} id - the job id
 * @param {IState} state - the state document
 */
function setJobState(id: string, state: IState) {
    let context: IContext = getContext();
    let collection: ICollection = context.getCollection();
    let response: IResponse = getContext().getResponse();
    let collectionLink: string = collection.getAltLink();
    let documentLink: string = `${collectionLink}/docs/${id}`;

    // convert the case for the data
    const keys: Array<string> = Object.keys(state.data);
    for (const key of keys) {
        const newKey = camelCaseToPascalCase(key);
        state.data[newKey] = state.data[key];
        delete state.data[key];
    }

    // default response
    response.setBody(false);

    let isAccepted: boolean = collection.readDocument(documentLink, (error: IRequestCallbackError, job: IJob) => {
        if (error) {
            throw error;
        }

        // now create the state document
        // on callback replace the job documented with state_id, state_name
        createState(state, (doc: IState): void => {
            job.state_id = doc.id;
            job.state_name = doc.name;
            let options: IReplaceOptions = { etag: job._etag };

            let success: boolean = collection.replaceDocument(job._self, job, options, (err: IRequestCallbackError) => {
                if (err) {
                    throw err;
                }
                response.setBody(true);
            });

            if (!success) {
                throw new Error("The call was not accepted");
            }
        });
    });

    /**
     * Creates a new state
     * @param {Object} doc - information for the job 
     * @param {function} callback - return the newly create state document
     */
    function createState(doc: IState, callback: (doc: IState) => void) {
        let success: boolean = collection.createDocument(collectionLink, doc, (error: IRequestCallbackError, document: IState) => {
            if (error) {
                throw error;
            }
            callback(document);
        });

        if (!success) {
            throw new Error("The call was not accepted");
        }
    }

    /**
     * Convert the camel case to pascal
     * @param input - The text which needs to be converted
     */
    function camelCaseToPascalCase(input: string): string {
        return input.replace(/([A-Z])/g, '$1')
            .replace(/^./, (match) => match.toUpperCase());
    }

    if (!isAccepted) {
        throw new Error("The call was not accepted");
    }
}