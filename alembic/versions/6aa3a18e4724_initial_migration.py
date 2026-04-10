import sqlalchemy as sa
from alembic import op

revision = '0db3520d2b8d'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    op.create_table('blacklist_rules',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('rule_type', sa.Text(), nullable=False),
    sa.Column('rule_value', sa.Text(), nullable=False),
    sa.Column('reason', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('markets',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('platform', sa.String(), nullable=False),
    sa.Column('platform_market_id', sa.String(), nullable=False),
    sa.Column('status', sa.String(), nullable=False),
    sa.Column('tradable', sa.Boolean(), nullable=True),
    sa.Column('title', sa.Text(), nullable=False),
    sa.Column('normalized_title', sa.Text(), nullable=False),
    sa.Column('description', sa.Text(), nullable=True),
    sa.Column('outcomes_json', sa.JSON(), nullable=False),
    sa.Column('raw_payload_json', sa.JSON(), nullable=False),
    sa.Column('category', sa.String(), nullable=True),
    sa.Column('slug', sa.String(), nullable=True),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('platform', 'platform_market_id', name='uq_markets_platform_market_id')
    )
    op.create_index('ix_markets_platform_status', 'markets', ['platform', 'status'], unique=False)
    op.create_table('settings',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('key', sa.String(), nullable=False),
    sa.Column('value_json', sa.JSON(), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('key')
    )
    op.create_table('market_pairs',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('market_id_a', sa.Integer(), nullable=False),
    sa.Column('market_id_b', sa.Integer(), nullable=False),
    sa.Column('pair_hash', sa.String(), nullable=False),
    sa.Column('status', sa.String(), nullable=False),
    sa.Column('match_score', sa.Float(), nullable=False),
    sa.Column('match_reason_json', sa.JSON(), nullable=True),
    sa.Column('outcome_mapping_json', sa.JSON(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['market_id_a'], ['markets.id'], ),
    sa.ForeignKeyConstraint(['market_id_b'], ['markets.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('pair_hash')
    )
    op.create_index('ix_market_pairs_market_id_a', 'market_pairs', ['market_id_a'], unique=False)
    op.create_index('ix_market_pairs_market_id_b', 'market_pairs', ['market_id_b'], unique=False)
    op.create_index('ix_market_pairs_status', 'market_pairs', ['status'], unique=False)
    op.create_table('users',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('status', sa.String(), nullable=False),
    sa.Column('role', sa.String(), nullable=False),
    sa.Column('plan_code', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_table('telegram_chats',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('chat_id', sa.String(), nullable=False),
    sa.Column('chat_type', sa.String(), nullable=False),
    sa.Column('is_primary', sa.Boolean(), nullable=False),
    sa.Column('is_verified', sa.Boolean(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('chat_id', name='uq_telegram_chats_chat_id')
    )
    op.create_table('user_preferences',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('min_roi_percent', sa.Float(), nullable=True),
    sa.Column('min_capital_usd', sa.Float(), nullable=True),
    sa.Column('max_capital_usd', sa.Float(), nullable=True),
    sa.Column('max_polymarket_capital_usd', sa.Float(), nullable=True),
    sa.Column('max_predict_fun_capital_usd', sa.Float(), nullable=True),
    sa.Column('min_profit_usd', sa.Float(), nullable=True),
    sa.Column('max_days_to_close', sa.Integer(), nullable=True),
    sa.Column('muted', sa.Boolean(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('user_id', name='uq_user_preferences_user_id')
    )
    op.create_table('subscriptions',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=False),
    sa.Column('channel', sa.String(), nullable=False),
    sa.Column('destination', sa.String(), nullable=False),
    sa.Column('status', sa.String(), nullable=False),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.Column('updated_at', sa.DateTime(timezone=True), nullable=False),
    sa.UniqueConstraint('channel', 'destination', name='uq_subscriptions_channel_destination'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.create_index('ix_subscriptions_user_id_status', 'subscriptions', ['user_id', 'status'], unique=False)
    op.create_table('arb_opportunities',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('market_pair_id', sa.Integer(), nullable=False),
    sa.Column('direction', sa.String(), nullable=False),
    sa.Column('price_leg_1', sa.Float(), nullable=False),
    sa.Column('price_leg_2', sa.Float(), nullable=False),
    sa.Column('avg_price_leg_1', sa.Float(), nullable=False),
    sa.Column('avg_price_leg_2', sa.Float(), nullable=False),
    sa.Column('shares', sa.Float(), nullable=False),
    sa.Column('capital_required', sa.Float(), nullable=False),
    sa.Column('gross_profit', sa.Float(), nullable=False),
    sa.Column('net_profit', sa.Float(), nullable=False),
    sa.Column('gross_roi', sa.Float(), nullable=False),
    sa.Column('net_roi', sa.Float(), nullable=False),
    sa.Column('calculation_json', sa.JSON(), nullable=True),
    sa.Column('fanout_status', sa.String(), nullable=False, server_default='queued'),
    sa.Column('fanout_processed_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('fanout_error_message', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['market_pair_id'], ['market_pairs.id'], ),
    sa.PrimaryKeyConstraint('id')
    )
    op.alter_column('arb_opportunities', 'fanout_status', server_default=None)
    op.create_index('ix_arb_opportunities_fanout_status_created_at', 'arb_opportunities', ['fanout_status', 'created_at'], unique=False)
    op.create_index('ix_arb_opportunities_market_pair_id', 'arb_opportunities', ['market_pair_id'], unique=False)
    op.create_index('ix_arb_opportunities_pair_direction_status', 'arb_opportunities', ['market_pair_id', 'direction', 'fanout_status'], unique=False)

    op.create_table('alerts',
    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
    sa.Column('opportunity_id', sa.Integer(), nullable=False),
    sa.Column('user_id', sa.Integer(), nullable=True),
    sa.Column('subscription_id', sa.Integer(), nullable=True),
    sa.Column('telegram_chat_id', sa.String(), nullable=False),
    sa.Column('message_hash', sa.String(), nullable=False),
    sa.Column('status', sa.String(), nullable=False),
    sa.Column('attempt_count', sa.Integer(), nullable=False, server_default='0'),
    sa.Column('next_retry_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('sent_at', sa.DateTime(timezone=True), nullable=True),
    sa.Column('error_message', sa.Text(), nullable=True),
    sa.Column('created_at', sa.DateTime(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(['opportunity_id'], ['arb_opportunities.id'], ),
    sa.ForeignKeyConstraint(['subscription_id'], ['subscriptions.id'], name='fk_alerts_subscription_id_subscriptions'),
    sa.ForeignKeyConstraint(['user_id'], ['users.id'], name='fk_alerts_user_id_users'),
    sa.PrimaryKeyConstraint('id'),
    sa.UniqueConstraint('opportunity_id', 'telegram_chat_id', name='uq_alerts_opportunity_chat')
    )
    op.alter_column('alerts', 'attempt_count', server_default=None)
    op.create_index('ix_alerts_opportunity_id', 'alerts', ['opportunity_id'], unique=False)
    op.create_index('ix_alerts_status_next_retry_at_id', 'alerts', ['status', 'next_retry_at', 'id'], unique=False)

def downgrade():
    op.drop_index('ix_alerts_status_next_retry_at_id', table_name='alerts')
    op.drop_index('ix_alerts_opportunity_id', table_name='alerts')
    op.drop_table('alerts')
    op.drop_index('ix_arb_opportunities_pair_direction_status', table_name='arb_opportunities')
    op.drop_index('ix_arb_opportunities_market_pair_id', table_name='arb_opportunities')
    op.drop_index('ix_arb_opportunities_fanout_status_created_at', table_name='arb_opportunities')
    op.drop_table('arb_opportunities')
    op.drop_index('ix_subscriptions_user_id_status', table_name='subscriptions')
    op.drop_table('subscriptions')
    op.drop_table('user_preferences')
    op.drop_table('telegram_chats')
    op.drop_table('users')
    op.drop_index('ix_market_pairs_status', table_name='market_pairs')
    op.drop_index('ix_market_pairs_market_id_b', table_name='market_pairs')
    op.drop_index('ix_market_pairs_market_id_a', table_name='market_pairs')
    op.drop_table('market_pairs')
    op.drop_table('settings')
    op.drop_index('ix_markets_platform_status', table_name='markets')
    op.drop_table('markets')
    op.drop_table('blacklist_rules')