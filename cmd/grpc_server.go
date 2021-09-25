package cmd

import (
	"context"
	"github.com/roseduan/rosedb"
	"github.com/roseduan/rosedb/cmd/proto"
	"log"
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
	panic("implement me")
}

func (g *GrpcServer) SDiff(_ context.Context, req *proto.SDiffReq) (*proto.SDiffRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) SKeyExists(_ context.Context, req *proto.SKeyExistsReq) (*proto.SKeyExistsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) SClear(_ context.Context, req *proto.SClearReq) (*proto.SClearRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) SExpire(_ context.Context, req *proto.SExpireReq) (*proto.SExpireRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) STTL(_ context.Context, req *proto.STTLReq) (*proto.STTLRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HSet(_ context.Context, req *proto.HSetReq) (*proto.HSetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HSetNx(_ context.Context, req *proto.HSetNxReq) (*proto.HSetNxRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HGet(_ context.Context, req *proto.HGetReq) (*proto.HGetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HGetAll(_ context.Context, req *proto.HGetAllReq) (*proto.HGetAllRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HMSet(_ context.Context, req *proto.HMSetReq) (*proto.HMSetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HMGet(_ context.Context, req *proto.HMGetReq) (*proto.HMGetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HDel(_ context.Context, req *proto.HDelReq) (*proto.HDelRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HKeyExists(_ context.Context, req *proto.HKeyExistsReq) (*proto.HKeyExistsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HExists(_ context.Context, req *proto.HExistsReq) (*proto.HExistsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HLen(_ context.Context, req *proto.HLenReq) (*proto.HLenRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HKeys(_ context.Context, req *proto.HKeysReq) (*proto.HKeysRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HVals(_ context.Context, req *proto.HValsReq) (*proto.HValsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HClear(_ context.Context, req *proto.HClearReq) (*proto.HClearRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HExpire(_ context.Context, req *proto.HExpireReq) (*proto.HExpireRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) HTTL(_ context.Context, req *proto.HTTLReq) (*proto.HTTLRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LPush(_ context.Context, req *proto.LPushReq) (*proto.LPushRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) RPush(_ context.Context, req *proto.RPushReq) (*proto.RPushRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LPop(_ context.Context, req *proto.LPopReq) (*proto.LPopRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) RPop(_ context.Context, req *proto.RPopReq) (*proto.RPopRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LIndex(_ context.Context, req *proto.LIndexReq) (*proto.LIndexRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LRem(_ context.Context, req *proto.LRemReq) (*proto.LRemRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LInsert(_ context.Context, req *proto.LInsertReq) (*proto.LInsertRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LSet(_ context.Context, req *proto.LSetReq) (*proto.LSetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LTrim(_ context.Context, req *proto.LTrimReq) (*proto.LTrimRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LRange(_ context.Context, req *proto.LRangeReq) (*proto.LRangeRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LLen(_ context.Context, req *proto.LLenReq) (*proto.LLenRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LKeyExists(_ context.Context, req *proto.LKeyExistsReq) (*proto.LKeyExistsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LValExists(_ context.Context, req *proto.LValExistsReq) (*proto.LValExistsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LClear(_ context.Context, req *proto.LClearReq) (*proto.LClearRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LExpire(_ context.Context, req *proto.LExpireReq) (*proto.LExpireRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) LTTL(_ context.Context, req *proto.LTTLReq) (*proto.LTTLRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) Set(_ context.Context, req *proto.SetReq) (*proto.SetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) SetNx(_ context.Context, req *proto.SetNxReq) (*proto.SetNxRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) SetEx(_ context.Context, req *proto.SetExReq) (*proto.SetExRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) Get(_ context.Context, req *proto.GetReq) (*proto.GetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) GetSet(_ context.Context, req *proto.GetSetReq) (*proto.GetSetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) MSet(_ context.Context, req *proto.MSetReq) (*proto.MSetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) MGet(_ context.Context, req *proto.MGetReq) (*proto.MGetRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) Append(_ context.Context, req *proto.AppendReq) (*proto.AppendRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) StrExists(_ context.Context, req *proto.StrExistsReq) (*proto.StrExistsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) Remove(_ context.Context, req *proto.RemoveReq) (*proto.RemoveRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) PrefixScan(_ context.Context, req *proto.PrefixScanReq) (*proto.PrefixScanRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) RangeScan(_ context.Context, req *proto.RangeScanReq) (*proto.RangeScanRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) Expire(_ context.Context, req *proto.ExpireReq) (*proto.ExpireRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) Persist(_ context.Context, req *proto.PersistReq) (*proto.PersistRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) TTL(_ context.Context, req *proto.TTLReq) (*proto.TTLRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZAdd(_ context.Context, req *proto.ZAddReq) (*proto.ZAddRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZScore(_ context.Context, req *proto.ZScoreReq) (*proto.ZScoreRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZCard(_ context.Context, req *proto.ZCardReq) (*proto.ZCardRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRank(_ context.Context, req *proto.ZRankReq) (*proto.ZRankRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRevRank(_ context.Context, req *proto.ZRevRankReq) (*proto.ZRevRankRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZIncrBy(_ context.Context, req *proto.ZIncrByReq) (*proto.ZIncrByRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRange(_ context.Context, req *proto.ZRangeReq) (*proto.ZRangeRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRangeWithScores(_ context.Context, req *proto.ZRangeWithScoresReq) (*proto.ZRangeWithScoresRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRevRange(_ context.Context, req *proto.ZRevRangeReq) (*proto.ZRevRangeRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRevRangeWithScores(_ context.Context, req *proto.ZRevRangeWithScoresReq) (*proto.ZRevRangeWithScoresRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRem(_ context.Context, req *proto.ZRemReq) (*proto.ZRemRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZGetByRank(_ context.Context, req *proto.ZGetByRankReq) (*proto.ZGetByRankRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRevGetByRank(_ context.Context, req *proto.ZRevGetByRankReq) (*proto.ZRevGetByRankRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZScoreRange(_ context.Context, req *proto.ZScoreRangeReq) (*proto.ZScoreRangeRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZRevScoreRange(_ context.Context, req *proto.ZRevScoreRangeReq) (*proto.ZRevScoreRangeRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZKeyExists(_ context.Context, req *proto.ZKeyExistsReq) (*proto.ZKeyExistsRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZClear(_ context.Context, req *proto.ZClearReq) (*proto.ZClearRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZExpire(_ context.Context, req *proto.ZExpireReq) (*proto.ZExpireRsp, error) {
	panic("implement me")
}

func (g *GrpcServer) ZTTL(_ context.Context, req *proto.ZTTLReq) (*proto.ZTTLRsp, error) {
	panic("implement me")
}
