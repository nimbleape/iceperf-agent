package client

import (
	"encoding/json"
	"time"

	"github.com/nimbleape/iceperf-agent/config"
	"github.com/nimbleape/iceperf-agent/util"
	"github.com/pion/stun/v2"
	"github.com/pion/webrtc/v4"
	log "github.com/sirupsen/logrus"
)

type ConnectionPair struct {
	OfferPC                 *webrtc.PeerConnection
	AnswerPC                *webrtc.PeerConnection
	LogOfferer              *log.Entry
	LogAnswerer             *log.Entry
	config                  *config.Config
	sentInitialMessageViaDC time.Time
	iceServerInfo           *stun.URI
}

func NewConnectionPair(config *config.Config, iceServerInfo *stun.URI) (c *ConnectionPair, err error) {
	return newConnectionPair(config, iceServerInfo)
}

func newConnectionPair(cc *config.Config, iceServerInfo *stun.URI) (*ConnectionPair, error) {

	logOfferer := cc.Logger.WithFields(log.Fields{
		"peer": "Offerer",
	})
	logAnswerer := cc.Logger.WithFields(log.Fields{
		"peer": "Answerer",
	})

	cp := &ConnectionPair{
		config:        cc,
		LogOfferer:    logOfferer,
		LogAnswerer:   logAnswerer,
		iceServerInfo: iceServerInfo,
	}

	config := webrtc.Configuration{}

	if cc.WebRTCConfig.ICEServers != nil {
		config.ICEServers = cc.WebRTCConfig.ICEServers
	}

	config.ICETransportPolicy = cc.WebRTCConfig.ICETransportPolicy
	config.SDPSemantics = webrtc.SDPSemanticsUnifiedPlanWithFallback

	//we only want offerer to force turn (if we are)
	cp.createOfferer(config)

	// think we want to leave the answerer without any ice servers so we only get the host candidates.... I think
	// to get the tests working I'm passing the turn server into both....
	// but I don't think that should be required
	cp.createAnswerer(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{
				URLs: []string{"stun:stun.l.google.com:19302"},
			},
		},
	})

	return cp, nil
}

func (cp *ConnectionPair) setRemoteDescription(pc *webrtc.PeerConnection, sdp []byte) {
	var desc webrtc.SessionDescription
	err := json.Unmarshal(sdp, &desc)
	util.Check(err)

	// Apply the desc as the remote description
	err = pc.SetRemoteDescription(desc)
	util.Check(err)
}

func (cp *ConnectionPair) createOfferer(config webrtc.Configuration) {
	// Create a new PeerConnection
	pc, err := webrtc.NewPeerConnection(config)
	util.Check(err)

	buf := make([]byte, 1024)

	ordered := false
	maxRetransmits := uint16(0)
	hasSentData := false

	options := &webrtc.DataChannelInit{
		Ordered:        &ordered,
		MaxRetransmits: &maxRetransmits,
	}

	sendMoreCh := make(chan struct{}, 1)

	// Create a datachannel with label 'data'
	dc, err := pc.CreateDataChannel("data", options)
	util.Check(err)

	if cp.iceServerInfo.Scheme == stun.SchemeTypeTURN || cp.iceServerInfo.Scheme == stun.SchemeTypeTURNS {

		// Register channel opening handling
		dc.OnOpen(func() {

			cp.LogOfferer.WithFields(log.Fields{
				"dataChannelLabel": dc.Label(),
				"dataChannelId":    dc.ID(),
			}).Info("OnOpen: Start sending a series of 1024-byte packets as fast as it can")

			for {
				if !hasSentData {
					cp.sentInitialMessageViaDC = time.Now()
					hasSentData = true
				}
				err2 := dc.Send(buf)
				util.Check(err2)

				if dc.BufferedAmount() > maxBufferedAmount {
					// Wait until the bufferedAmount becomes lower than the threshold
					<-sendMoreCh
				}
			}
		})

		// Set bufferedAmountLowThreshold so that we can get notified when
		// we can send more
		dc.SetBufferedAmountLowThreshold(bufferedAmountLowThreshold)

		// This callback is made when the current bufferedAmount becomes lower than the threshold
		dc.OnBufferedAmountLow(func() {
			// Make sure to not block this channel or perform long running operations in this callback
			// This callback is executed by pion/sctp. If this callback is blocking it will stop operations
			select {
			case sendMoreCh <- struct{}{}:
			default:
			}
		})

		dc.OnClose(func() {
			bytesSentTotal, _ := getDataChannelBytesSent(pc, dc)

			cp.LogAnswerer.WithFields(log.Fields{
				"sentBytesTotal": bytesSentTotal,
			}).Info("Sent total")
		})

	}
	cp.OfferPC = pc
}

func (cp *ConnectionPair) createAnswerer(config webrtc.Configuration) {
	// Create a new PeerConnection
	pc, err := webrtc.NewPeerConnection(config)
	util.Check(err)

	if cp.iceServerInfo.Scheme == stun.SchemeTypeTURN || cp.iceServerInfo.Scheme == stun.SchemeTypeTURNS {

		pc.OnDataChannel(func(dc *webrtc.DataChannel) {
			var totalBytesReceived uint64

			hasRecievedData := false

			// Register channel opening handling
			dc.OnOpen(func() {
				cp.LogAnswerer.WithFields(log.Fields{
					"dataChannelLabel": dc.Label(),
					"dataChannelId":    dc.ID(),
				}).Info("OnOpen: Start receiving data")

				since := time.Now()

				// Start printing out the observed throughput
				for range time.NewTicker(100 * time.Millisecond).C {
					//check if this pc is closed and break out
					if pc.ConnectionState() != webrtc.PeerConnectionStateConnected {
						break
					}
					bps := 8 * float64(totalBytesReceived) / float64(time.Since(since).Seconds())
					// bps := float64(totalBytesReceived*8) / time.Since(since).Seconds()
					cp.LogAnswerer.WithFields(log.Fields{
						"throughput": bps / (1024*1024),
						"eventTime":  time.Now(),
					}).Info("On ticker: Calculated throughput")
				}

				bps := 8 * float64(totalBytesReceived) / float64(time.Since(since).Seconds())

				// bps := float64(totalBytesReceived*8) / time.Since(since).Seconds()
				cp.LogAnswerer.WithFields(log.Fields{
					"throughput":       bps / (1024*1024),
					"eventTime":        time.Now(),
					"timeSinceStartMs": time.Since(since).Milliseconds(),
				}).Info("On ticker: Calculated throughput")
			})

			// Register the OnMessage to handle incoming messages
			dc.OnMessage(func(dcMsg webrtc.DataChannelMessage) {
				if !hasRecievedData {
					cp.LogAnswerer.WithFields(log.Fields{
						"latencyFirstPacketInMs": time.Since(cp.sentInitialMessageViaDC).Milliseconds(),
					}).Info("Received first Packet")
					hasRecievedData = true
				}
				totalBytesReceivedTmp, ok := getDataChannelBytesReceived(pc, dc)
				if ok {
					totalBytesReceived = totalBytesReceivedTmp
				}
			})

			dc.OnClose(func() {
				bytesReceivedTotal, _ := getDataChannelBytesReceived(pc, dc)
				cp.LogAnswerer.WithFields(log.Fields{
					"receivedBytesTotal": bytesReceivedTotal,
				}).Info("Received total")
			})
		})
	}

	cp.AnswerPC = pc
}

func getDataChannelBytesReceived(pc *webrtc.PeerConnection, dc *webrtc.DataChannel) (uint64, bool) {
	stats := pc.GetStats()
	dcStats, ok := stats.GetDataChannelStats(dc)
	if !ok {
		return 0, ok
	}
	return dcStats.BytesReceived, ok
}

func getDataChannelBytesSent(pc *webrtc.PeerConnection, dc *webrtc.DataChannel) (uint64, bool) {
	stats := pc.GetStats()
	dcStats, ok := stats.GetDataChannelStats(dc)
	if !ok {
		return 0, ok
	}
	return dcStats.BytesSent, ok
}
