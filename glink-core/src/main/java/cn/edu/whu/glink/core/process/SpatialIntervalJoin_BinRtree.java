package cn.edu.whu.glink.core.process;

import cn.edu.whu.glink.core.datastream.SpatialDataStream;
import cn.edu.whu.glink.core.enums.TopologyType;
import cn.edu.whu.glink.core.index.RTreeIndexWithTime;
import cn.edu.whu.glink.core.index.TreeIndexWithTime;
import cn.edu.whu.glink.core.operator.grid.GeometryDistributedGridMapper;
import cn.edu.whu.glink.core.operator.grid.GeometryGridMapper;
import cn.edu.whu.glink.core.operator.join.JoinWithTopologyType;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkException;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author Lynn Lee
 */
public class SpatialIntervalJoin_BinRtree {

    @SuppressWarnings("unchecked")
    public static <T1 extends Geometry, T2 extends Geometry> SingleOutputStreamOperator join(
            SpatialDataStream<T1> leftStream,
            SpatialDataStream<T2> rightStream,
            TopologyType joinType,
            Time lowerBound,
            Time upperBound,
            int binSizeInt) {

        DataStream<Tuple2<Long, T1>> stream1 =
                leftStream.getDataStream().flatMap(new GeometryGridMapper<>());
        DataStream<Tuple2<Long, T2>> stream2 =
                rightStream.getDataStream().flatMap(new GeometryDistributedGridMapper<>(joinType));

        return stream1
                .connect(stream2)
                .keyBy(t -> t.f0, t -> t.f0)
                .process(new SpatialIntervalJoinFunc(lowerBound, upperBound, joinType, binSizeInt))
                .returns(TypeInformation.of(new TypeHint<Tuple2<Geometry, Geometry>>() {
                }));

    }

    public static class SpatialIntervalJoinFunc<T1 extends Geometry, T2 extends Geometry>
            extends KeyedCoProcessFunction<Long, Tuple2<Long, T1>, Tuple2<Long, T2>, Tuple2<T1, T2>> {
        private final long lowerBoundMs;
        private final long upperBoundMs;
        private final TopologyType joinType;
        private final JoinFunction<T1, T2, Tuple2<T1, T2>> joinFunction;
        private transient MapState<Long, TreeIndexWithTime<T1>> leftBuffer;
        private transient MapState<Long, TreeIndexWithTime<T2>> rightBuffer;
        private transient MapState<Long, List<String>> cleanNameSpace;
        private static final String LEFT_BUFFER = "LEFT_BUFFER";
        private static final String RIGHT_BUFFER = "RIGHT_BUFFER";
        private static final String CLEANUP_NAMESPACE_LEFT = "CLEANUP_LEFT";
        private static final String CLEANUP_NAMESPACE_RIGHT = "CLEANUP_RIGHT";
        private final long binSize;

        public SpatialIntervalJoinFunc(Time lowerBound, Time upperBound, TopologyType joinType, int binSizeInt) {
            this.lowerBoundMs = lowerBound.toMilliseconds();
            this.upperBoundMs = upperBound.toMilliseconds();
            this.joinType = joinType;
            this.joinFunction = Tuple2::new;
            this.binSize = Time.minutes(binSizeInt).toMilliseconds();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            this.leftBuffer = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>(
                            LEFT_BUFFER,
                            TypeInformation.of(Long.class),
                            TypeInformation.of(new TypeHint<TreeIndexWithTime<T1>>() {
                            })));
            this.rightBuffer = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>(
                            RIGHT_BUFFER,
                            TypeInformation.of(Long.class),
                            TypeInformation.of(new TypeHint<TreeIndexWithTime<T2>>() {
                            })
                    ));
            this.cleanNameSpace = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<>(
                            "CLEAN_NAME_SPACE",
                            TypeInformation.of(Long.class),
                            TypeInformation.of(new TypeHint<List<String>>() {
                            })
                    ));
        }

        @Override
        public void processElement1(Tuple2<Long, T1> leftStream, KeyedCoProcessFunction<Long, Tuple2<Long, T1>, Tuple2<Long, T2>, Tuple2<T1, T2>>.Context ctx, Collector<Tuple2<T1, T2>> out) throws Exception {
            final T1 leftValue = leftStream.f1;
            final long leftTimeStamp = ctx.timestamp();
            if (leftTimeStamp == Long.MIN_VALUE) {
                throw new FlinkException(
                        "Long.MIN_VALUE timestamp: Elements used in "
                                + "interval stream joins need to have timestamps meaningful timestamps.");
            }
            // add to buffer
            long leftBinIndex = getBinIndex(leftTimeStamp);
            RTreeIndexWithTime<T1> thisTimeRTree = (RTreeIndexWithTime<T1>) leftBuffer.get(leftBinIndex);
            if (thisTimeRTree == null) {
                thisTimeRTree = new RTreeIndexWithTime<>();
                long leftBinTimestampMax = getBinMaxTimestamp(leftBinIndex);
                long cleanupTime = (upperBoundMs > 0L) ? leftBinTimestampMax + upperBoundMs : leftBinTimestampMax;
                ctx.timerService().registerEventTimeTimer(cleanupTime);
                List<String> tmpList = cleanNameSpace.get(cleanupTime);
                if (tmpList == null) {
                    tmpList = new ArrayList<>();
                }
                tmpList.add(CLEANUP_NAMESPACE_LEFT);
                cleanNameSpace.put(cleanupTime, tmpList);
            }
            thisTimeRTree.insert(leftValue, leftTimeStamp);
            leftBuffer.put(leftBinIndex, thisTimeRTree);
            // search the right buffer
            for (Map.Entry<Long, TreeIndexWithTime<T2>> rightTree : rightBuffer.entries()) {
                final long rightBinIndex = rightTree.getKey();
                if (rightBinIndex < getBinIndex(leftTimeStamp + lowerBoundMs)
                        || rightBinIndex > getBinIndex(leftTimeStamp + upperBoundMs)) {
                    continue;
                }
                if (joinType == TopologyType.WITHIN_DISTANCE) {
                    for (T2 geoms : rightTree
                            .getValue()
                            .query(leftValue,
                                    leftTimeStamp + lowerBoundMs,
                                    leftTimeStamp + upperBoundMs,
                                    joinType.getDistance(),
                                    SpatialDataStream.distanceCalculator)) {
                        out.collect(joinFunction.join(leftValue, geoms));
                    }
                } else {
                    for (T2 geoms : rightTree.getValue().query(leftValue, leftTimeStamp + lowerBoundMs, leftTimeStamp + upperBoundMs)) {
                        JoinWithTopologyType
                                .join(leftValue, geoms, joinType, joinFunction, SpatialDataStream.distanceCalculator)
                                .ifPresent(out::collect);
                    }
                }

            }

        }

        @Override
        public void processElement2(Tuple2<Long, T2> rightStream, KeyedCoProcessFunction<Long, Tuple2<Long, T1>, Tuple2<Long, T2>, Tuple2<T1, T2>>.Context ctx, Collector<Tuple2<T1, T2>> out) throws Exception {
            final T2 rightValue = rightStream.f1;
            final long rightTimeStamp = ctx.timestamp();

            if (rightTimeStamp == Long.MIN_VALUE) {
                throw new FlinkException(
                        "Long.MIN_VALUE timestamp: Elements used in "
                                + "interval stream joins need to have timestamps meaningful timestamps.");
            }
            // add to buffer
            long rightBinIndex = getBinIndex(rightTimeStamp);

            RTreeIndexWithTime<T2> thisTimeRTree = (RTreeIndexWithTime<T2>) rightBuffer.get(rightBinIndex);
            if (thisTimeRTree == null) {
                thisTimeRTree = new RTreeIndexWithTime<>();
                //clean up
                long rightBinTimestampMax = getBinMaxTimestamp(rightBinIndex);
                long cleanupTime = (lowerBoundMs < 0L) ? rightBinTimestampMax - lowerBoundMs : rightBinTimestampMax;
                ctx.timerService().registerEventTimeTimer(cleanupTime);
                List<String> tmpList = cleanNameSpace.get(cleanupTime);
                if (tmpList == null) {
                    tmpList = new ArrayList<>();
                }
                tmpList.add(CLEANUP_NAMESPACE_RIGHT);
                cleanNameSpace.put(cleanupTime, tmpList);
            }
            thisTimeRTree.insert(rightValue, rightTimeStamp);
            rightBuffer.put(rightBinIndex, thisTimeRTree);
            // search the left buffer
            for (Map.Entry<Long, TreeIndexWithTime<T1>> leftTree : leftBuffer.entries()) {
                final long leftBinIndex = leftTree.getKey();
                if (getBinIndex(rightTimeStamp - lowerBoundMs) < leftBinIndex
                        || getBinIndex(rightTimeStamp - upperBoundMs) > leftBinIndex) {
                    continue;
                }
                if (joinType == TopologyType.WITHIN_DISTANCE) {
                    for (T1 geoms : leftTree
                            .getValue()
                            .query(rightValue,
                                    rightTimeStamp - upperBoundMs,
                                    rightTimeStamp - lowerBoundMs,
                                    joinType.getDistance(),
                                    SpatialDataStream.distanceCalculator)) {
                        out.collect(joinFunction.join(geoms, rightValue));
                    }
                } else {
                    for (T1 geoms : leftTree.getValue().query(rightValue, rightTimeStamp - upperBoundMs, rightTimeStamp - lowerBoundMs)) {
                        JoinWithTopologyType
                                .join(geoms, rightValue, joinType, joinFunction, SpatialDataStream.distanceCalculator)
                                .ifPresent(out::collect);
                    }
                }

            }

        }

        @Override
        public void onTimer(long timestamp, KeyedCoProcessFunction<Long, Tuple2<Long, T1>, Tuple2<Long, T2>, Tuple2<T1, T2>>.OnTimerContext ctx, Collector<Tuple2<T1, T2>> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            for (String s : cleanNameSpace.get(timestamp)) {
                if (s.equals(CLEANUP_NAMESPACE_LEFT)) {
                    long cleanLeft =
                            (upperBoundMs <= 0L) ? getBinIndex(timestamp) : getBinIndex(timestamp - upperBoundMs);
                    leftBuffer.remove(cleanLeft);
                } else if (s.equals(CLEANUP_NAMESPACE_RIGHT)) {
                    long cleanRight =
                            (lowerBoundMs <= 0L) ? getBinIndex(timestamp + lowerBoundMs) : getBinIndex(timestamp);
                    rightBuffer.remove(cleanRight);
                }
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            leftBuffer.clear();
            rightBuffer.clear();
            cleanNameSpace.clear();
        }

        private long getBinIndex(long timestamp) {
            return (long) Math.ceil((double) timestamp / (double) binSize);
        }

        private long getBinMaxTimestamp(long binIndex) {
            return binIndex * binSize;
        }

        private boolean notInTimeInterval(long leftTime, long rightTime) {
            return (rightTime < (leftTime + lowerBoundMs)) || (rightTime > (leftTime + upperBoundMs));
        }


    }
}
