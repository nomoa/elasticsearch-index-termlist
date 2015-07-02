package org.xbib.elasticsearch.action.termlist;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.xbib.elasticsearch.common.termlist.CompactHashMap;
import org.xbib.elasticsearch.common.termlist.math.SummaryStatistics;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

/**
 * Termlist index/indices action.
 */
public class TransportTermlistAction
        extends TransportBroadcastOperationAction<TermlistRequest, TermlistResponse, ShardTermlistRequest, ShardTermlistResponse> {

    private final static ESLogger logger = ESLoggerFactory.getLogger(TransportTermlistAction.class.getName());

    private final IndicesService indicesService;

    @Inject
    public TransportTermlistAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   TransportService transportService,
                                   IndicesService indicesService,
                                   ActionFilters actionFilters) {
        super(settings, TermlistAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected TermlistRequest newRequest() {
        return new TermlistRequest();
    }

    @Override
    protected TermlistResponse newResponse(TermlistRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        int numdocs = 0;
        Map<String, TermInfo> map = new CompactHashMap<String, TermInfo>();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse instanceof BroadcastShardOperationFailedException) {
                BroadcastShardOperationFailedException e = (BroadcastShardOperationFailedException)shardResponse;
                logger.error(e.getMessage(), e);
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newLinkedList();
                }
                shardFailures.add(new DefaultShardOperationFailedException(e));
            } else {
                if (shardResponse instanceof ShardTermlistResponse) {
                    successfulShards++;
                    ShardTermlistResponse resp = (ShardTermlistResponse) shardResponse;
                    numdocs += resp.getNumDocs();
                    Map<String, TermInfo> smallMap = resp.getTermList();
                    if(smallMap.size() > map.size()) {
                        smallMap = map;
                        map = resp.getTermList();
                    }
                    update(map, smallMap);
                }
            }
        }
        map = request.sortByTotalFreq() ? sortTotalFreq(map, request.getFrom(), request.getSize()) :
                request.sortByDocFreq() ? sortDocFreq(map, request.getFrom(), request.getSize()) :
                        request.sortByTerm() ? sortTerm(map, request.getFrom(), request.getSize()) :
                                request.getSize() >= 0 ? truncate(map, request.getFrom(), request.getSize()) : map;

        return new TermlistResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures, numdocs, map);
    }

    @Override
    protected ShardTermlistRequest newShardRequest() {
        return new ShardTermlistRequest();
    }

    @Override
    protected ShardTermlistRequest newShardRequest(int numShards, ShardRouting shard, TermlistRequest request) {
        return new ShardTermlistRequest(shard.getIndex(), shard.shardId(), request);
    }

    @Override
    protected ShardTermlistResponse newShardResponse() {
        return new ShardTermlistResponse();
    }

    /**
     * The termlist request works against primary shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, TermlistRequest request, String[] concreteIndices) {
        return clusterState.routingTable().activePrimaryShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermlistRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, TermlistRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

    @Override
    protected ShardTermlistResponse shardOperation(ShardTermlistRequest request) throws ElasticsearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.getIndex()).shardSafe(request.shardId().id());
        Engine.Searcher searcher = indexShard.engine().acquireSearcher("termlist");
        try {
            Map<String, TermInfo> map = new CompactHashMap<String, TermInfo>();
            IndexReader reader = searcher.reader();
            Fields fields = MultiFields.getFields(reader);
            if (fields != null) {
                for (String field : fields) {
                    // skip internal fields
                    if (field.charAt(0) == '_') {
                        continue;
                    }
                    if (request.getRequest().getField() == null || field.equals(request.getRequest().getField())) {
                        Terms terms = fields.terms(field);
                        // Returns the number of documents that have at least one
                        if (terms != null) {
                            TermsEnum termsEnum = terms.iterator(null);
                            BytesRef text;
                            while ((text = termsEnum.next()) != null) {
                                // skip invalid terms
                                if (termsEnum.docFreq() < 1) {
                                    continue;
                                }
                                // docFreq() = the number of documents containing the current term
                                // totalTermFreq() = total number of occurrences of this term across all documents
                                Term term = new Term(field, text);
                                if (request.getRequest().getTerm() == null || term.text().startsWith(request.getRequest().getTerm())) {
                                    TermInfo termInfo = new TermInfo();
                                    DocsEnum docPosEnum = termsEnum.docs(null, null);
                                    SummaryStatistics stat = new SummaryStatistics();
                                    while (docPosEnum.nextDoc() != DocsEnum.NO_MORE_DOCS) {
                                        stat.addValue(docPosEnum.freq());
                                    }
                                    termInfo.setSummaryStatistics(stat);
                                    termInfo.setDocFreq(termsEnum.docFreq());
                                    termInfo.setTotalFreq(termsEnum.totalTermFreq() > 0 ? termsEnum.totalTermFreq() : 0);
                                    map.put(term.text(), termInfo);
                                }
                            }
                        }
                    }
                }
            }
            return new ShardTermlistResponse(request.getIndex(), request.shardId(), reader.numDocs(), map);
        } catch (Throwable ex) {
            logger.error(ex.getMessage(), ex);
            throw new ElasticsearchException(ex.getMessage(), ex);
        } finally {
            searcher.close();
        }
    }

    private void update(Map<String, TermInfo> map, Map<String, TermInfo> other) {
        for (Map.Entry<String, TermInfo> tentry2 : other.entrySet()) {
            TermInfo t2 = tentry2.getValue();
            TermInfo t1 = map.put(tentry2.getKey(), t2);
            if (t1 != null) {
                Long totalFreq = t2.getTotalFreq();
                if (totalFreq != null) {
                    if (t1.getTotalFreq() != null) {
                        t2.setTotalFreq(totalFreq + t1.getTotalFreq());
                    }
                } else {
                    if (t1.getTotalFreq() != null) {
                        t2.setTotalFreq(t1.getTotalFreq());
                    }
                }
                Integer docFreq = t2.getDocFreq();
                if (docFreq != null) {
                    if (t1.getDocFreq() != null) {
                        t2.setDocFreq(docFreq + t1.getDocFreq());
                    }
                } else {
                    if (t1.getDocFreq() != null) {
                        t2.setDocFreq(t1.getDocFreq());
                    }
                }
                SummaryStatistics summaryStatistics = t2.getSummaryStatistics();
                if (summaryStatistics != null) {
                    if (t1.getSummaryStatistics() != null) {
                        summaryStatistics.update(t1.getSummaryStatistics());
                    }
                } else {
                    if (t1.getSummaryStatistics() != null) {
                        t2.setSummaryStatistics(t1.getSummaryStatistics());
                    }
                }
            }
        }
    }

    private SortedMap<String, TermInfo> sortTerm(final Map<String, TermInfo> map, Integer from, Integer size) {
        Comparator<String> comp = new Comparator<String>() {
            @Override
            public int compare(String t1, String t2) {
                return t1.compareTo(t2);
            }
        };
        TreeMap<String, TermInfo> m = new TreeMap<String, TermInfo>(comp);
        m.putAll(map);
        if (size != null && size > 0) {
            TreeMap<String, TermInfo> treeMap = new TreeMap<String, TermInfo>(comp);
            for (int i = 0; i < m.size(); i++) {
                Map.Entry<String, TermInfo> me = m.pollFirstEntry();
                if (from <= i && i < from + size) {
                    treeMap.put(me.getKey(), me.getValue());
                }
            }
            return treeMap;
        }
        return m;
    }
    private SortedMap<String, TermInfo> sortTotalFreq(final Map<String, TermInfo> map, Integer from, Integer size) {
        Comparator<String> comp = new Comparator<String>() {
            @Override
            public int compare(String t1, String t2) {
                Long l1 = map.get(t1).getTotalFreq();
                String sl1 = Long.toString(l1);
                String s1 = sl1.length() + sl1 + t1;
                Long l2 = map.get(t2).getTotalFreq();
                String sl2 = Long.toString(l2);
                String s2 =sl2.length() + sl2 + t2;
                return -s1.compareTo(s2);
            }
        };
        TreeMap<String, TermInfo> m = new TreeMap<String, TermInfo>(comp);
        m.putAll(map);
        if (size != null && size > 0) {
            TreeMap<String, TermInfo> treeMap = new TreeMap<String, TermInfo>(comp);
            for (int i = 0; i < m.size(); i++) {
                Map.Entry<String, TermInfo> me = m.pollFirstEntry();
                if (from <= i && i < from + size) {
                    treeMap.put(me.getKey(), me.getValue());
                }
            }
            return treeMap;
        }
        return m;
    }

    private SortedMap<String, TermInfo> sortDocFreq(final Map<String, TermInfo> map, Integer from, Integer size) {
        Comparator<String> comp = new Comparator<String>() {
            @Override
            public int compare(String t1, String t2) {
                Integer i1 = map.get(t1).getDocFreq();
                String si1 = Integer.toString(i1);
                String s1 = si1.length() + si1 + t1;
                Integer i2 = map.get(t2).getDocFreq();
                String si2 = Integer.toString(i2);
                String s2 = si2.length() + si2 + t2;
                return -s1.compareTo(s2);
            }
        };
        TreeMap<String, TermInfo> m = new TreeMap<String, TermInfo>(comp);
        m.putAll(map);
        if (size != null && size > 0) {
            TreeMap<String, TermInfo> treeMap = new TreeMap<String, TermInfo>(comp);
            for (int i = 0; i < m.size(); i++) {
                Map.Entry<String, TermInfo> me = m.pollFirstEntry();
                if (from <= i && i < from + size) {
                    treeMap.put(me.getKey(), me.getValue());
                }
            }
            return treeMap;
        }
        return m;
    }

    private Map<String, TermInfo> truncate(Map<String, TermInfo> source, Integer from, Integer size) {
        if (size == null || size < 1) {
            return source;
        }
        Map<String, TermInfo> target = new CompactHashMap<String, TermInfo>();
        Iterator<Map.Entry<String, TermInfo>> it = source.entrySet().iterator();
        for (int i = 0 ; i < source.size(); i++) {
            Map.Entry<String, TermInfo> entry = it.next();
            if (from <= i && i < from + size) {
                target.put(entry.getKey(), entry.getValue());
            }
        }
        return target;
    }

}
