const { Kafka } = require('kafkajs');
const { pubClient } = require('../utils/redisClient');

const aiService = require('../services/aiService');

const kafka = new Kafka({
  clientId: 'ai-worker',
  brokers: [process.env.KAFKA_HOST],
});

const consumer = kafka.consumer({ groupId: 'Ktb-consumer-group' });

const run = async () => {
    try {
        await consumer.connect();
        console.log('Kafka consumer 연결 성공');
      } catch (err) {
        console.error('kafka consumer 연결 실패:', err);
      }
  await consumer.subscribe({ topic: 'ai-requests', fromBeginning: false });
  consumer.on(consumer.events.GROUP_JOIN, (e) => {
    console.log(`[GROUP_JOIN] Consumer joined group:`, e);
  });
  
  consumer.on(consumer.events.CONNECT, () => {
    console.log(`[CONNECTED] Kafka consumer connected`);
  });
  
  consumer.on(consumer.events.CRASH, (e) => {
    console.error(`[CRASH]`, e.payload.error);
  });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log(`[CONSUME] topic=${topic}, partition=${partition}`);
      try {
        
        console.log(`[CONSUME VALUE]`, message.value.toString());
        const data = JSON.parse(message.value.toString());
        const { roomId, aiType, query, user, timestamp } = data;
        const messageId = `${aiType}-${Date.now()}`;

        // Redis: 시작 알림
        await pubClient.publish(`room:${roomId}`, JSON.stringify({
          type: 'aiMessageStart',
          data: { messageId, aiType, timestamp }
        }));
        let accumulatedContent = ''; 
        // AI 응답 생성
        await aiService.generateResponse(query, aiType, {
            onStart: async () => {
                await pubClient.publish(`room:${roomId}`, JSON.stringify({
                    type: 'aiMessageStart',
                    data: { messageId, aiType, timestamp }
                }));
                },
                onChunk: async (chunk) => {
                    accumulatedContent += chunk.currentChunk || '';
                  
                    await pubClient.publish(`room:${roomId}`, JSON.stringify({
                      type: 'aiMessageChunk',
                      data: {
                        messageId,
                        currentChunk: chunk.currentChunk,
                        fullContent: accumulatedContent, // 🔥 이게 지금 undefined였던 원인
                        isCodeBlock: chunk.isCodeBlock,
                        aiType,
                        timestamp: new Date(),
                      },
                    }));
                  },
          onComplete: async (finalContent) => {
            await pubClient.publish(`room:${roomId}`, JSON.stringify({
              type: 'aiMessageComplete',
              data: {
                messageId,
                content: finalContent.content,
                isComplete: true,
                query,
                aiType,
                timestamp: new Date()
              }
            }));
          },
          onError: async (err) => {
            await pubClient.publish(`room:${roomId}`, JSON.stringify({
              type: 'aiMessageError',
              data: {
                messageId,
                aiType,
                error: err.message
              }
            }));
          }
        });

      } catch (error) {
        console.error('Kafka message handling failed:', error);
      }
    },
  });
};




module.exports = { run };