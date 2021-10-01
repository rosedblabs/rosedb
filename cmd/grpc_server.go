package cmd

import (
	"context"
	"github.com/roseduan/rosedb"
	"github.com/roseduan/rosedb/cmd/proto"
	"github.com/roseduan/rosedb/ds/list"
	"github.com/roseduan/rosedb/utils"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
)

type GrpcServer struct {
	db     *rosedb.RoseDB
	closed bool
	mu     sync.Mutex
}

func NewGrpcServer(db *rosedb.RoseDB) *GrpcServer {
	return &GrpcServer{
		db: db,
		closed: false,
	}
}

func (g *GrpcServer) Listen(addr string) {
	s := grpc.NewServer()
	proto.RegisterRosedbServer(s, g)
	lis, err := net.Listen("tcp", addr)
	log.Printf("grpc server serve in addr %s", addr)
	if err != nil {
		log.Printf("listen to %s err: %+v", addr, err)
		return
	}
	if err = s.Serve(lis); err != nil {
		log.Printf("grpc server err: %+v", err)
		return
	}
}

func (g *GrpcServer) Stop() {
	if g.closed {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.closed = true
	if err := g.db.Close(); err != nil {
		log.Printf("close rosedb err: %+v\n", err)
	}
}

func (g *GrpcServer) SAdd(_ context.Context,req *proto.SAddReq) (*proto.SAddRsp, error) {
	rsp := &proto.SAddRsp{}
	var resInt int
	resInt, err := g.db.SAdd(req.Key, req.Members ...)
	rsp.Res = int64(resInt)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SPop(_ context.Context,req *proto.SPopReq) (rsp *proto.SPopRsp,err error) {
	rsp = &proto.SPopRsp{}
	rsp.Values, err = g.db.SPop(req.Key, int(req.Count))
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SIsMember(_ context.Context,req *proto.SIsMemberReq) (*proto.SIsMemberRsp, error) {
	rsp := &proto.SIsMemberRsp{}
	rsp.IsMember = g.db.SIsMember(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) SRandMember(_ context.Context,req *proto.SRandMemberReq) (*proto.SRandMemberRsp, error) {
	rsp := &proto.SRandMemberRsp{}
	g.db.SRandMember(req.Key, int(req.Count))
	return rsp, nil
}

func (g *GrpcServer) SRem(_ context.Context, req *proto.SRemReq) (*proto.SRemRsp, error) {
	rsp := &proto.SRemRsp{}
	resInt, err := g.db.SRem(req.Key, req.Members...)
	rsp.Res = int64(resInt)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SMove(_ context.Context, req *proto.SMoveReq) (*proto.SMoveRsp, error) {
	rsp := &proto.SMoveRsp{}
	err := g.db.SMove(req.Src, req.Dst, req.Member)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SCard(_ context.Context, req *proto.SCardReq) (*proto.SCardRsp, error) {
	rsp := &proto.SCardRsp{}
	count := g.db.SCard(req.Key)
	rsp.Res = int64(count)
	return rsp, nil
}

func (g *GrpcServer) SMembers(_ context.Context, req *proto.SMembersReq) (*proto.SMembersRsp, error) {
	rsp := &proto.SMembersRsp{}
	rsp.Values = g.db.SMembers(req.Key)
	return rsp, nil
}

func (g *GrpcServer) SUnion(_ context.Context, req *proto.SUnionReq) (*proto.SUnionRsp, error) {
	rsp := &proto.SUnionRsp{}
	rsp.Values = g.db.SUnion(req.Keys...)
	return rsp, nil
}

func (g *GrpcServer) SDiff(_ context.Context, req *proto.SDiffReq) (*proto.SDiffRsp, error) {
	rsp := &proto.SDiffRsp{}
	rsp.Values = g.db.SDiff(req.Keys...)
	return rsp, nil
}

func (g *GrpcServer) SKeyExists(_ context.Context, req *proto.SKeyExistsReq) (*proto.SKeyExistsRsp, error) {
	rsp := &proto.SKeyExistsRsp{}
	rsp.Ok = g.db.SKeyExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) SClear(_ context.Context, req *proto.SClearReq) (*proto.SClearRsp, error) {
	rsp := &proto.SClearRsp{}
	rsp.Ok = g.db.SKeyExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) SExpire(_ context.Context, req *proto.SExpireReq) (*proto.SExpireRsp, error) {
	rsp := &proto.SExpireRsp{}
	err := g.db.SExpire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) STTL(_ context.Context, req *proto.STTLReq) (*proto.STTLRsp, error) {
	rsp := &proto.STTLRsp{}
	rsp.Ttl = g.db.STTL(req.Key)
	return rsp, nil
}

func (g *GrpcServer) HSet(_ context.Context, req *proto.HSetReq) (*proto.HSetRsp, error) {
	rsp := &proto.HSetRsp{}
	resInt, err := g.db.HSet(req.Key, req.Field, req.Value)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) HSetNx(_ context.Context, req *proto.HSetNxReq) (*proto.HSetNxRsp, error) {
	rsp := &proto.HSetNxRsp{}
	resInt, err := g.db.HSetNx(req.Key, req.Field, req.Value)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) HGet(_ context.Context, req *proto.HGetReq) (*proto.HGetRsp, error) {
	rsp := &proto.HGetRsp{}
	rsp.Value = g.db.HGet(req.Key, req.Field)
	return rsp, nil
}

func (g *GrpcServer) HGetAll(_ context.Context, req *proto.HGetAllReq) (*proto.HGetAllRsp, error) {
	rsp := &proto.HGetAllRsp{}
	rsp.Values = g.db.HGetAll(req.Key)
	return rsp, nil
}

func (g *GrpcServer) HMSet(_ context.Context, req *proto.HMSetReq) (*proto.HMSetRsp, error) {
	rsp := &proto.HMSetRsp{}
	err := g.db.HMSet(req.Key, req.Values...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) HMGet(_ context.Context, req *proto.HMGetReq) (*proto.HMGetRsp, error) {
	rsp := &proto.HMGetRsp{}
	rsp.Values = g.db.HMGet(req.Key, req.Fields...)
	return rsp, nil
}

func (g *GrpcServer) HDel(_ context.Context, req *proto.HDelReq) (*proto.HDelRsp, error) {
	rsp := &proto.HDelRsp{}
	resInt ,err := g.db.HDel(req.Key, req.Fields...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) HKeyExists(_ context.Context, req *proto.HKeyExistsReq) (*proto.HKeyExistsRsp, error) {
	rsp := &proto.HKeyExistsRsp{}
	rsp.Ok = g.db.HKeyExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) HExists(_ context.Context, req *proto.HExistsReq) (*proto.HExistsRsp, error) {
	rsp := &proto.HExistsRsp{}
	rsp.Ok = g.db.HExists(req.Key, req.Field)
	return rsp, nil
}

func (g *GrpcServer) HLen(_ context.Context, req *proto.HLenReq) (*proto.HLenRsp, error) {
	rsp := &proto.HLenRsp{}
	length := g.db.HLen(req.Key)
	rsp.Length = int64(length)
	return rsp, nil
}

func (g *GrpcServer) HKeys(_ context.Context, req *proto.HKeysReq) (*proto.HKeysRsp, error) {
	rsp := &proto.HKeysRsp{}
	valuesStr := g.db.HKeys(req.Key)
	rsp.Values = make([][]byte, 0)
	for _, v := range valuesStr {
		rsp.Values = append(rsp.Values, []byte(v))
	}
	return rsp, nil
}

func (g *GrpcServer) HVals(_ context.Context, req *proto.HValsReq) (*proto.HValsRsp, error) {
	rsp := &proto.HValsRsp{}
	rsp.Values = g.db.HVals(req.Key)
	return rsp, nil
}

func (g *GrpcServer) HClear(_ context.Context, req *proto.HClearReq) (*proto.HClearRsp, error) {
	rsp := &proto.HClearRsp{}
	err := g.db.HClear(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) HExpire(_ context.Context, req *proto.HExpireReq) (*proto.HExpireRsp, error) {
	rsp := &proto.HExpireRsp{}
	err := g.db.HExpire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) HTTL(_ context.Context, req *proto.HTTLReq) (*proto.HTTLRsp, error) {
	rsp := &proto.HTTLRsp{}
	rsp.Ttl = g.db.HTTL(req.Key)
	return rsp, nil
}

func (g *GrpcServer) LPush(_ context.Context, req *proto.LPushReq) (*proto.LPushRsp, error) {
	rsp := &proto.LPushRsp{}
	resInt, err := g.db.LPush(req.Key, req.Values...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) RPush(_ context.Context, req *proto.RPushReq) (*proto.RPushRsp, error) {
	rsp := &proto.RPushRsp{}
	resInt, err := g.db.RPush(req.Key, req.Values...)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) LPop(_ context.Context, req *proto.LPopReq) (*proto.LPopRsp, error) {
	rsp := &proto.LPopRsp{}
	var err error
	rsp.Value, err = g.db.LPop(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) RPop(_ context.Context, req *proto.RPopReq) (*proto.RPopRsp, error) {
	rsp := &proto.RPopRsp{}
	var err error
	rsp.Value, err = g.db.RPop(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil

}

func (g *GrpcServer) LIndex(_ context.Context, req *proto.LIndexReq) (*proto.LIndexRsp, error) {
	rsp := &proto.LIndexRsp{}
	rsp.Value = g.db.LIndex(req.Key, int(req.Idx))
	return rsp, nil
}

func (g *GrpcServer) LRem(_ context.Context, req *proto.LRemReq) (*proto.LRemRsp, error) {
	rsp := &proto.LRemRsp{}
	resInt, err := g.db.LRem(req.Key, req.Value, int(req.Count))
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Res = int64(resInt)
	return rsp, nil
}

func (g *GrpcServer) LInsert(_ context.Context, req *proto.LInsertReq) (*proto.LInsertRsp, error) {
	rsp := &proto.LInsertRsp{}
	count, err := g.db.LInsert(string(req.Key), list.InsertOption(req.Option), req.Pivot, req.Value)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Count = int64(count)
	return rsp, nil
}

func (g *GrpcServer) LSet(_ context.Context, req *proto.LSetReq) (*proto.LSetRsp, error) {
	rsp := &proto.LSetRsp{}
	var err error
	rsp.Ok, err = g.db.LSet(req.Key, int(req.Idx), req.Value)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) LTrim(_ context.Context, req *proto.LTrimReq) (*proto.LTrimRsp, error) {
	rsp := &proto.LTrimRsp{}
	err := g.db.LTrim(req.Key, int(req.Start), int(req.End))
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) LRange(_ context.Context, req *proto.LRangeReq) (*proto.LRangeRsp, error) {
	rsp := &proto.LRangeRsp{}
	var err error
	rsp.Values, err = g.db.LRange(req.Key, int(req.Start), int(req.End))
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil

}

func (g *GrpcServer) LLen(_ context.Context, req *proto.LLenReq) (*proto.LLenRsp, error) {
	rsp := &proto.LLenRsp{}
	rsp.Length = int64(g.db.LLen(req.Key))
	return rsp, nil
}

func (g *GrpcServer) LKeyExists(_ context.Context, req *proto.LKeyExistsReq) (*proto.LKeyExistsRsp, error) {
	rsp := &proto.LKeyExistsRsp{}
	rsp.Ok = g.db.LKeyExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) LValExists(_ context.Context, req *proto.LValExistsReq) (*proto.LValExistsRsp, error) {
	rsp := &proto.LValExistsRsp{}
	rsp.Ok = g.db.LValExists(req.Key, req.Value)
	return rsp, nil
}

func (g *GrpcServer) LClear(_ context.Context, req *proto.LClearReq) (*proto.LClearRsp, error) {
	rsp := &proto.LClearRsp{}
	err := g.db.LClear(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) LExpire(_ context.Context, req *proto.LExpireReq) (*proto.LExpireRsp, error) {
	rsp := &proto.LExpireRsp{}
	err := g.db.LExpire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) LTTL(_ context.Context, req *proto.LTTLReq) (*proto.LTTLRsp, error) {
	rsp := &proto.LTTLRsp{}
	rsp.Ttl = g.db.LTTL(req.Key)
	return rsp, nil
}

func (g *GrpcServer) Set(_ context.Context, req *proto.SetReq) (*proto.SetRsp, error) {
	rsp := &proto.SetRsp{}
	err := g.db.Set(req.Key, req.Value)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) SetNx(_ context.Context, req *proto.SetNxReq) (*proto.SetNxRsp, error) {
	rsp := &proto.SetNxRsp{}
	ok, err := g.db.SetNx(req.Key, req.Value)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	rsp.Ok = ok
	return rsp, nil
}

func (g *GrpcServer) SetEx(_ context.Context, req *proto.SetExReq) (*proto.SetExRsp, error) {
	rsp := &proto.SetExRsp{}
	err := g.db.SetEx(req.Key, req.Value, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) Get(_ context.Context, req *proto.GetReq) (*proto.GetRsp, error) {
	rsp := &proto.GetRsp{}
	err := g.db.Get(req.Key, &rsp.Dest)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) GetSet(_ context.Context, req *proto.GetSetReq) (*proto.GetSetRsp, error) {
	rsp := &proto.GetSetRsp{}
	err := g.db.GetSet(req.Key, req.Value, rsp.Dest)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) MSet(_ context.Context, req *proto.MSetReq) (*proto.MSetRsp, error) {
	rsp := &proto.MSetRsp{}
	if len(req.Keys) != len(req.Values) {
		rsp.ErrorMsg = "len(keys) != len(values)"
		return rsp, nil
	}
	mulkv := make([][]byte, 0)
	for i := 0; i < len(req.Keys); i++ {
		mulkv = append(mulkv, req.Keys[i])
		mulkv = append(mulkv, req.Values[i])
	}
 	err := g.db.MSet(mulkv)
 	if err != nil {
 		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) MGet(_ context.Context, req *proto.MGetReq) (*proto.MGetRsp, error) {
	rsp := &proto.MGetRsp{}
	values, err := g.db.MGet(req.Keys)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	} else {
		rsp.Values = values
	}
	return rsp, nil
}

func (g *GrpcServer) Append(_ context.Context, req *proto.AppendReq) (*proto.AppendRsp, error) {
	rsp := &proto.AppendRsp{}
	err := g.db.Append(req.Key, string(req.Value))
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) StrExists(_ context.Context, req *proto.StrExistsReq) (*proto.StrExistsRsp, error) {
	rsp := &proto.StrExistsRsp{}
	rsp.Ok = g.db.StrExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) Remove(_ context.Context, req *proto.RemoveReq) (*proto.RemoveRsp, error) {
	rsp := &proto.RemoveRsp{}
	err := g.db.Remove(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) PrefixScan(_ context.Context, req *proto.PrefixScanReq) (*proto.PrefixScanRsp, error) {
	rsp := &proto.PrefixScanRsp{}
	valueInterface, err := g.db.PrefixScan(string(req.Prefix), int(req.Limit), int(req.Offset))
	if err != nil {
		rsp.ErrorMsg = err.Error()
		return rsp, nil
	}
	rsp.Values = make([][]byte, 0)
	for _, v := range valueInterface {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		}
		rsp.Values = append(rsp.Values, vEncode)
	}
	return rsp, nil
}

func (g *GrpcServer) RangeScan(_ context.Context, req *proto.RangeScanReq) (*proto.RangeScanRsp, error) {
	rsp := &proto.RangeScanRsp{}
	rsp.Values = make([][]byte, 0)
	valueInterface, err := g.db.RangeScan(req.Start, req.End)
	if err != nil {
		rsp.ErrorMsg = err.Error()
		return rsp, nil
	}
	rsp.Values = make([][]byte, 0)
	for _, v := range valueInterface {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		}
		rsp.Values = append(rsp.Values, vEncode)
	}
	return rsp, nil
}

func (g *GrpcServer) Expire(_ context.Context, req *proto.ExpireReq) (*proto.ExpireRsp, error) {
	rsp := &proto.ExpireRsp{}
	err := g.db.Expire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) Persist(_ context.Context, req *proto.PersistReq) (*proto.PersistRsp, error) {
	rsp := &proto.PersistRsp{}
	err := g.db.Persist(req.Key)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) TTL(_ context.Context, req *proto.TTLReq) (*proto.TTLRsp, error) {
	rsp := &proto.TTLRsp{}
	rsp.Ttl = g.db.TTL(req.Key)
	return rsp, nil
}

func (g *GrpcServer) ZAdd(_ context.Context, req *proto.ZAddReq) (*proto.ZAddRsp, error) {
	rsp := &proto.ZAddRsp{}
	err := g.db.ZAdd(req.Key, req.Score, req.Member)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) ZScore(_ context.Context, req *proto.ZScoreReq) (*proto.ZScoreRsp, error) {
	rsp := &proto.ZScoreRsp{}
	rsp.Ok, rsp.Score = g.db.ZScore(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) ZCard(_ context.Context, req *proto.ZCardReq) (*proto.ZCardRsp, error) {
	rsp := &proto.ZCardRsp{}
	sz := g.db.ZCard(req.Key)
	rsp.Size = int64(sz)
	return rsp, nil
}

func (g *GrpcServer) ZRank(_ context.Context, req *proto.ZRankReq) (*proto.ZRankRsp, error) {
	rsp := &proto.ZRankRsp{}
	rsp.Rank = g.db.ZRank(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) ZRevRank(_ context.Context, req *proto.ZRevRankReq) (*proto.ZRevRankRsp, error) {
	rsp := &proto.ZRevRankRsp{}
	rsp.Rank = g.db.ZRevRank(req.Key, req.Member)
	return rsp, nil
}

func (g *GrpcServer) ZIncrBy(_ context.Context, req *proto.ZIncrByReq) (*proto.ZIncrByRsp, error) {
	rsp := &proto.ZIncrByRsp{}
	var err error
	rsp.Scores, err = g.db.ZIncrBy(req.Key, req.Increment, req.Member)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) ZRange(_ context.Context, req *proto.ZRangeReq) (*proto.ZRangeRsp, error) {
	rsp := &proto.ZRangeRsp{}
	values := g.db.ZRange(req.Key, int(req.Start), int(req.End))
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZRangeWithScores(_ context.Context, req *proto.ZRangeWithScoresReq) (*proto.ZRangeWithScoresRsp, error) {
	rsp := &proto.ZRangeWithScoresRsp{}
	values := g.db.ZRangeWithScores(req.Key, int(req.Start), int(req.End))
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZRevRange(_ context.Context, req *proto.ZRevRangeReq) (*proto.ZRevRangeRsp, error) {
	rsp := &proto.ZRevRangeRsp{}
	values := g.db.ZRevRange(req.Key, int(req.Start), int(req.End))
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZRevRangeWithScores(_ context.Context, req *proto.ZRevRangeWithScoresReq) (*proto.ZRevRangeWithScoresRsp, error) {
	rsp := &proto.ZRevRangeWithScoresRsp{}
	values := g.db.ZRevRangeWithScores(req.Key, int(req.Start), int(req.End))
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZRem(_ context.Context, req *proto.ZRemReq) (*proto.ZRemRsp, error) {
	rsp := &proto.ZRemRsp{}
	ok, err := g.db.ZRem(req.Key, req.Member)
	if err != nil {
		rsp.ErrorMsg = err.Error()
		return rsp, nil
	}
	rsp.Ok = ok
	return rsp, nil
}

func (g *GrpcServer) ZGetByRank(_ context.Context, req *proto.ZGetByRankReq) (*proto.ZGetByRankRsp, error) {
	rsp := &proto.ZGetByRankRsp{}
	values := g.db.ZGetByRank(req.Key, int(req.Rank))
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZRevGetByRank(_ context.Context, req *proto.ZRevGetByRankReq) (*proto.ZRevGetByRankRsp, error) {
	rsp := &proto.ZRevGetByRankRsp{}
	values := g.db.ZRevGetByRank(req.Key, int(req.Rank))
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZScoreRange(_ context.Context, req *proto.ZScoreRangeReq) (*proto.ZScoreRangeRsp, error) {
	rsp := &proto.ZScoreRangeRsp{}
	values := g.db.ZScoreRange(req.Key, req.Min, req.Max)
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZRevScoreRange(_ context.Context, req *proto.ZRevScoreRangeReq) (*proto.ZRevScoreRangeRsp, error) {
	rsp := &proto.ZRevScoreRangeRsp{}
	values := g.db.ZRevScoreRange(req.Key, req.Max, req.Min)
	rsp.Values = make([][]byte, 0)
	for _, v := range values {
		vEncode, err := utils.EncodeValue(v)
		if err != nil {
			rsp.ErrorMsg = err.Error()
			return rsp, nil
		} else {
			rsp.Values = append(rsp.Values, vEncode)
		}
	}
	return rsp, nil
}

func (g *GrpcServer) ZKeyExists(_ context.Context, req *proto.ZKeyExistsReq) (*proto.ZKeyExistsRsp, error) {
	rsp := &proto.ZKeyExistsRsp{}
	rsp.Ok = g.db.ZKeyExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) ZClear(_ context.Context, req *proto.ZClearReq) (*proto.ZClearRsp, error) {
	rsp := &proto.ZClearRsp{}
	rsp.Ok = g.db.ZKeyExists(req.Key)
	return rsp, nil
}

func (g *GrpcServer) ZExpire(_ context.Context, req *proto.ZExpireReq) (*proto.ZExpireRsp, error) {
	rsp := &proto.ZExpireRsp{}
	err := g.db.ZExpire(req.Key, req.Duration)
	if err != nil {
		rsp.ErrorMsg = err.Error()
	}
	return rsp, nil
}

func (g *GrpcServer) ZTTL(_ context.Context, req *proto.ZTTLReq) (*proto.ZTTLRsp, error) {
	rsp := &proto.ZTTLRsp{}
	rsp.Ttl = g.db.ZTTL(req.Key)
	return rsp, nil
}
