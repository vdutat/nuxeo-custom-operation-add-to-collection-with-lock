package com.acme.automation.operation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.nuxeo.ecm.automation.core.Constants;
import org.nuxeo.ecm.automation.core.annotations.Context;
import org.nuxeo.ecm.automation.core.annotations.Operation;
import org.nuxeo.ecm.automation.core.annotations.OperationMethod;
import org.nuxeo.ecm.automation.core.annotations.Param;
import org.nuxeo.ecm.collections.api.CollectionManager;
import org.nuxeo.ecm.core.api.ConcurrentUpdateException;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.DocumentRef;
import org.nuxeo.ecm.core.api.Lock;
import org.nuxeo.ecm.core.api.LockHelper;

/**
 *
 */
@Operation(id=AddToCollectionWithLock.ID, category=Constants.CAT_DOCUMENT, label="Lock collection and add document(s) to it", description="Describe here what your operation does.")
public class AddToCollectionWithLock {

    public static final String ID = "Document.AddToCollectionWithLock";

    @Context
    protected CoreSession session;

    @Context
    protected CollectionManager collectionManager;

    @Param(name = "collection")
    protected DocumentModel collection;

    @Param(name = "lockcollection", required = false, values = "true")
    protected boolean lockCollection = true;

    private static final Log log = LogFactory.getLog(AddToCollectionWithLock.class);

    @OperationMethod
    public DocumentModelList run(DocumentModelList docs) {
        for (DocumentModel doc : docs) {
            run(doc);
        }
        return session.getDocuments((DocumentRef[]) docs.stream().map(doc -> doc.getRef()).toArray());
    }

    @OperationMethod()
    public DocumentModel run(DocumentModel doc) {
        if (lockCollection) {
            boolean retry = false;
            try {
                doLockandAdd(doc);
            } catch (ConcurrentUpdateException e) {
                retry = true;
                log.error("You should retry " + doc, e);
            } finally {
                if (retry) {
                    log.warn("Retrying " + doc);
                    doLockandAdd(doc);
                }
            }
        } else {
            collectionManager.addToCollection(collection, doc, session);
        }
        return session.getDocument(doc.getRef());
    }
    
    protected void doLockandAdd(DocumentModel doc) {
        String key = computeKeyForAtomicCreation(collection);
        LockHelper.doAtomically(key, () -> {
            while (collection.isLocked()) {
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Waiting for lock on " + collection);
                    }
                    wait(1L);
                } catch (InterruptedException e) {
                    if (log.isDebugEnabled()) {
                        log.debug("Interrupted");
                    }
                }
            }
            Lock lock = session.setLock(collection.getRef());
            if (log.isDebugEnabled()) {
                log.debug("Lock: " + lock);
                log.debug("Lock on " + collection);
            }
            collection = session.getDocument(collection.getRef());
            collectionManager.addToCollection(collection, doc, session);
            lock = session.removeLock(collection.getRef());
            if (log.isDebugEnabled()) {
                log.debug("Unlocked " + collection);
            }
        });        
    }

    protected String computeKeyForAtomicCreation(DocumentModel docModel) {
        String repositoryName = docModel.getRepositoryName();
        return repositoryName + "-" + docModel.getId();
    }
}
