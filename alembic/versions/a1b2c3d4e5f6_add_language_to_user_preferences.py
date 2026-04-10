import sqlalchemy as sa
from alembic import op

revision = 'a1b2c3d4e5f6'
down_revision = '0db3520d2b8d'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('user_preferences', sa.Column('language', sa.String(), nullable=True))

def downgrade():
    op.drop_column('user_preferences', 'language')
