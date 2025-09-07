from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, BigInteger, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class PerformerDimension(Base):
    """Performer dimension table"""
    __tablename__ = 'performer_dimension'
    
    performer_key = Column(String, primary_key=True)
    performer_name = Column(String, nullable=False)
    external_link = Column(String, nullable=False)
    followers_total = Column(Integer, nullable=False)
    profile_image = Column(String, nullable=False)
    popularity_index = Column(Integer, nullable=False)
    
    def __repr__(self):
        return f"<PerformerDimension(performer_key='{self.performer_key}', performer_name='{self.performer_name}')>"

class ReleaseDimension(Base):
    """Release dimension table"""
    __tablename__ = 'release_dimension'
    
    release_key = Column(String, primary_key=True)
    release_name = Column(String, nullable=False)
    track_count = Column(Integer, nullable=False)
    publish_date = Column(DateTime, nullable=False)
    external_link = Column(String, nullable=False)
    cover_image = Column(String, nullable=False)
    publisher_name = Column(String, nullable=False)
    popularity_index = Column(Integer, nullable=False)
    
    def __repr__(self):
        return f"<ReleaseDimension(release_key='{self.release_key}', release_name='{self.release_name}')>"

class TrackDimension(Base):
    """Track dimension table"""
    __tablename__ = 'track_dimension'
    
    track_key = Column(String, primary_key=True)
    track_name = Column(String, nullable=False)
    disc_position = Column(Integer, nullable=False)
    length_milliseconds = Column(BigInteger, nullable=False)
    contains_explicit = Column(Boolean, nullable=False)
    external_link = Column(String, nullable=False)
    sample_url = Column(String, nullable=True)
    popularity_index = Column(Integer, nullable=False)
    
    def __repr__(self):
        return f"<TrackDimension(track_key='{self.track_key}', track_name='{self.track_name}')>"

class ListeningFact(Base):
    """Fact table for listening events"""
    __tablename__ = 'listening_fact'
    
    event_key = Column(Integer, primary_key=True, autoincrement=True)
    timestamp_played = Column(DateTime, nullable=False)
    track_key = Column(String, ForeignKey('track_dimension.track_key'), nullable=False)
    release_key = Column(String, ForeignKey('release_dimension.release_key'), nullable=False)
    performer_key = Column(String, ForeignKey('performer_dimension.performer_key'), nullable=False)
    
    # Relationships
    track_info = relationship("TrackDimension")
    release_info = relationship("ReleaseDimension")
    performer_info = relationship("PerformerDimension")
    
    def __repr__(self):
        return f"<ListeningFact(event_key={self.event_key}, timestamp_played='{self.timestamp_played}')>"